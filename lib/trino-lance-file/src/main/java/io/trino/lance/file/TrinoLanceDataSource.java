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
package io.trino.lance.file;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class TrinoLanceDataSource
        implements LanceDataSource
{
    private final LanceDataSourceId id;
    private final FileFormatDataSourceStats stats;
    private final TrinoInput input;
    private final long estimatedSize;
    private long readTimeNanos;
    private long readBytes;

    public TrinoLanceDataSource(TrinoInputFile file, FileFormatDataSourceStats stats)
            throws IOException
    {
        this.id = new LanceDataSourceId(file.location().toString());
        this.stats = requireNonNull(stats, "stats is null");
        this.input = file.newInput();
        this.estimatedSize = file.length();
    }

    @Override
    public LanceDataSourceId getId()
    {
        return id;
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getEstimatedSize()
    {
        return estimatedSize;
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        long start = System.nanoTime();
        Slice tail = input.readTail(length);
        stats.readDataBytesPerSecond(tail.length(), System.nanoTime() - start);
        readTimeNanos += System.nanoTime() - start;
        readBytes += tail.length();
        return tail;
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        long start = System.nanoTime();
        byte[] buffer = new byte[length];
        input.readFully(position, buffer, 0, length);
        stats.readDataBytesPerSecond(length, System.nanoTime() - start);
        readTimeNanos += System.nanoTime() - start;
        readBytes += length;
        return Slices.wrappedBuffer(buffer);
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }
}
