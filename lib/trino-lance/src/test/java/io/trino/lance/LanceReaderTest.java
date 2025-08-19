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
package io.trino.lance;

import com.google.common.collect.ImmutableList;
import com.lancedb.lance.file.LanceFileReader;
import com.lancedb.lance.file.LanceFileWriter;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.nio.file.Files.createTempDirectory;

class LanceReaderTest
{
    @Test
    public void testFileRead()
            throws IOException
    {
        String path = "/Users/hluo/Downloads/manytypes.lance";
//        String path = "/Users/hluo/workspace/oss/lance/temp_data.lance/data/fc8e357c-e9c8-4fc9-b861-3da63eb0e9d8.lance";
        LanceDataSource dataSource = new FileLanceDataSource(new File(path));

        try (LanceReader reader = new LanceReader(dataSource, ImmutableList.of(5), Optional.empty())) {
            reader.getFields().forEach(System.out::println);
            SourcePage page = reader.nextSourcePage();
            Block block = page.getPage().getBlock(0);

            double[] output = new double[block.getPositionCount()];
            for (int i = 0; i < page.getPositionCount(); i++) {
                output[i] = DOUBLE.getDouble(block, i);
            }
            System.out.println(output);
        }

        try (LanceReader reader = new LanceReader(dataSource, ImmutableList.of(6), Optional.empty())) {
            reader.getFields().forEach(System.out::println);
            SourcePage page = reader.nextSourcePage();
            Block block = page.getPage().getBlock(0);

            String[] output = new String[block.getPositionCount()];
            for (int i = 0; i < page.getPositionCount(); i++) {
                output[i] = VarcharType.VARCHAR.getSlice(block, i).toStringUtf8();
            }
            System.out.println(output);
        }

        try (LanceReader reader = new LanceReader(dataSource, ImmutableList.of(7), Optional.empty())) {
            reader.getFields().forEach(System.out::println);
            SourcePage page = reader.nextSourcePage();
            Block block = page.getPage().getBlock(0);

            String[] output = new String[block.getPositionCount()];
            for (int i = 0; i < page.getPositionCount(); i++) {
                output[i] = VarcharType.VARCHAR.getSlice(block, i).toStringUtf8();
            }
            System.out.println(output);
        }

        try (LanceReader reader = new LanceReader(dataSource, ImmutableList.of(8), Optional.empty())) {
            reader.getFields().forEach(System.out::println);
            SourcePage page = reader.nextSourcePage();
            Block block = page.getPage().getBlock(0);

            String[] output = new String[block.getPositionCount()];
            for (int i = 0; i < page.getPositionCount(); i++) {
                output[i] = VarbinaryType.VARBINARY.getSlice(block, i).toStringUtf8();
            }
            System.out.println(output);
        }
    }

    @Test
    public void testJNIReader()
            throws IOException
    {
        BufferAllocator allocator = new RootAllocator();
        String file = "/Users/hluo/workspace/tmp/files/debug.lance";
        LanceFileReader reader = LanceFileReader.open(file, allocator);
        try (ArrowReader batches = reader.readAll(null, null, 100)) {
            while (batches.loadNextBatch()) {
                VectorSchemaRoot batch = batches.getVectorSchemaRoot();
                batch.getVector(0).getFieldBuffers().stream().forEach(arrowBuf -> arrowBuf.readByte());
            }
        }
    }

    @Test
    public void testJNIWriteStruct()
            throws IOException
    {
        Path tempDir = createTempDirectory(null);
        File file = tempDir.resolve("data.lance").toFile();

        try (RootAllocator allocator = new RootAllocator()) {
            LanceFileWriter writer = LanceFileWriter.open(file.getPath(), allocator, null);
            // Define struct fields: a, b, c (all bigint)
            Field fieldA = new Field("a", FieldType.notNullable(new ArrowType.Int(64, true)), null);
            Field fieldB = new Field("b", FieldType.notNullable(new ArrowType.Int(64, true)), null);
            Field fieldC = new Field("c", FieldType.notNullable(new ArrowType.Int(64, true)), null);

            // Define struct field "s"
            Field structField = new Field("s", FieldType.notNullable(ArrowType.Struct.INSTANCE), ImmutableList.of(fieldA, fieldB, fieldC));
            Field ssf = new Field("ss", FieldType.notNullable(ArrowType.Struct.INSTANCE), ImmutableList.of(structField));

            Schema schema = new Schema(ImmutableList.of(ssf));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                StructVector ssv = (StructVector) root.getVector("ss");
                ssv.setInitialCapacity(1);
                ssv.addOrGet("s", structField.getFieldType(), StructVector.class);
                ssv.allocateNew();
                ssv.setIndexDefined(0);
                StructVector structVector = (StructVector) ssv.getChild("s");
                structVector.setInitialCapacity(1); // set capacity to at least 1
                structVector.addOrGet("a", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
                structVector.addOrGet("b", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
                structVector.addOrGet("c", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
                structVector.allocateNew();

                BigIntVector aVector = (BigIntVector) structVector.getChild("a");
                BigIntVector bVector = (BigIntVector) structVector.getChild("b");
                BigIntVector cVector = (BigIntVector) structVector.getChild("c");

                // Set values at index 0
                structVector.setIndexDefined(0);
                aVector.setSafe(0, 100L);
                bVector.setSafe(0, 200L);
                cVector.setSafe(0, 300L);
                structVector.setIndexDefined(1);
                aVector.setSafe(1, 1100L);
                bVector.setSafe(1, 1200L);
                cVector.setSafe(1, 1300L);

                structVector.setValueCount(2);
                ssv.setValueCount(1);
                root.setRowCount(1);
                writer.write(root);
                writer.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        LanceDataSource dataSource = new FileLanceDataSource(file);
        Type trinoType = RowType.from(ImmutableList.of(RowType.field("s", RowType.from(ImmutableList.of(RowType.field("a", BigintType.BIGINT), RowType.field("b", BigintType.BIGINT), RowType.field("c", BigintType.BIGINT))))));
        try (LanceReader reader = new LanceReader(dataSource, ImmutableList.of(0), Optional.empty())) {
            for (SourcePage page = reader.nextSourcePage(); page != null; page = reader.nextSourcePage()) {
                int batchSize = page.getPositionCount();
                Block block = page.getBlock(0);
                List<Object> data = new ArrayList<>(block.getPositionCount());
                Object value = trinoType.getObjectValue(block, 0);
                System.out.println(batchSize);
                System.out.println(value);
            }
        }
    }

    @Test
    public void testJNIWriteList()
            throws IOException
    {
        Path tempDir = createTempDirectory(null);
        File file = tempDir.resolve("data.lance").toFile();

        // Example: Writing a list column to Lance file
        // This demonstrates how to write a list column containing bigint elements

        // Step 1: Define the data structure
        // We want to write a list column with bigint elements

        // Step 2: Create test data
        // Each element is a list of bigint values
        List<?> values = ImmutableList.of(ImmutableList.of(1L, 2L, 3L),      // First row: [1, 2, 3]
                ImmutableList.of(1L, 2L, 3L),         // Second row: [10, 20]
                ImmutableList.of(1L, 2L, 3L));  // Third row: [100, 200, 300, 400]

        try (RootAllocator allocator = new RootAllocator()) {
            LanceFileWriter writer = LanceFileWriter.open(file.getPath(), allocator, null);

            // Step 3: Define the Arrow schema
            // Create a field for bigint elements
            Field elementField = new Field("element", FieldType.notNullable(new ArrowType.Int(64, true)), null);
            // Create a list field containing bigint elements
            Field listField = new Field("list_column", FieldType.notNullable(ArrowType.List.INSTANCE), ImmutableList.of(elementField));

            Schema schema = new Schema(ImmutableList.of(listField));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();

                // Step 4: Get the list vector and set up the writer
                ListVector listVector = (ListVector) root.getVector("list_column");

                if (false) {
                    listVector.setInitialCapacity(values.size());
                    // Step 5: Write the data using UnionListWriter
                    UnionListWriter listWriter = listVector.getWriter();

                    for (int i = 0; i < values.size(); i++) {
                        List<?> listValue = (List<?>) values.get(i);

                        // Start writing a list
                        listWriter.startList();

                        // Write each element in the list
                        for (Object element : listValue) {
                            Long longValue = (Long) element;
                            listWriter.bigInt().writeBigInt(longValue);
                        }

                        // End the list
                        listWriter.endList();
                    }

                    // Step 6: Set the value count and write to file
                    listWriter.setValueCount(values.size());
                    listVector.setValueCount(values.size());
                }
                else {
                    listVector.setInitialCapacity(values.size());
                    UnionListWriter listWriter = listVector.getWriter();
                    for (Object list : values) {
                        listWriter.startList();
                        for (Object value : (List<?>) list) {
                            listWriter.bigInt().writeBigInt((Long) value);
                        }
                        listWriter.endList();
                    }
                    listWriter.setValueCount(values.size());
                    listVector.setValueCount(values.size());
                }

                root.setRowCount(values.size());
                try {
                    writer.write(root);
                }
                catch (Exception e) {
                    throw new IOException("Failed to write to Lance file", e);
                }
                try {
                    writer.close();
                }
                catch (Exception e) {
                    throw new IOException("Failed to close Lance file writer", e);
                }
            }
        }

        // Step 7: Verify the data was written successfully
        System.out.println("Successfully wrote Lance file: " + file.getPath());
        System.out.println("File size: " + file.length() + " bytes");
        System.out.println("Expected data written:");
        for (int i = 0; i < values.size(); i++) {
            List<?> listValue = (List<?>) values.get(i);
            System.out.println("Row " + i + ": " + listValue);
        }

        // Clean up - files will be automatically cleaned up when tempDir is garbage collected
    }
}
