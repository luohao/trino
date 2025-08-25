/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.lance.v2;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.lance.LanceDataSource;
import io.trino.lance.LanceDataSourceId;
import io.trino.lance.LanceReader;
import io.trino.lance.MemoryLanceDataSource;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkColumnReaders.ROWS)
public class BenchmarkColumnReaders
{
    public static final int ROWS = 10_000_000;

    @Benchmark
    public Object readTinyIntNoNull(TinyIntNoNullBenchmarkData data)
            throws Exception
    {
        try (LanceReader recordReader = data.createReader()) {
            return readColumn(recordReader);
        }
    }

    @Benchmark
    public Object readTinyIntWithNull(TinyIntWithNullBenchmarkData data)
            throws Exception
    {
        try (LanceReader recordReader = data.createReader()) {
            return readColumn(recordReader);
        }
    }

    private Object readColumn(LanceReader reader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        for (SourcePage page = reader.nextSourcePage(); page != null; page = reader.nextSourcePage()) {
            blocks.add(page.getBlock(0));
        }
        return blocks;
    }

    @State(Thread)
    public static class TinyIntNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT, createValues(), false);
        }

        private Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add((byte) ThreadLocalRandom.current().nextInt());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class TinyIntWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT, createValues(), false);
        }

        private Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                int value = ThreadLocalRandom.current().nextInt();
                if (value % 7 == 0) {
                    values.add(null);
                }
                values.add((byte) value);
            }
            return values.iterator();
        }
    }

    public abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private Path temporaryDirectory;
        private File lanceFile;
        private LanceDataSource dataSource;

        public void setup(Type type, Iterator<?> values, boolean nullable)
                throws Exception
        {
            this.type = type;
            temporaryDirectory = createTempDirectory(null);
            lanceFile = temporaryDirectory.resolve(randomUUID().toString()).toFile();
            LanceTester.writeLanceColumnJNI(lanceFile, type, newArrayList(values), nullable);
            dataSource = new MemoryLanceDataSource(new LanceDataSourceId(lanceFile.getPath()), Slices.wrappedBuffer(readAllBytes(lanceFile.toPath())));
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory, ALLOW_INSECURE);
        }

        LanceReader createReader()
                throws IOException
        {
            return new LanceReader(dataSource, ImmutableList.of(0), Optional.empty());
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkColumnReaders.class).run();
    }
}
