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
package io.trino.lance.v2.reader;

import io.trino.lance.LanceDataSource;
import io.trino.lance.v2.metadata.ColumnMetadata;
import io.trino.lance.v2.metadata.Field;
import io.trino.lance.v2.metadata.LogicalType;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Map;

public interface ColumnReader
{
    static ColumnReader createColumnReader(LanceDataSource dataSource, Field field, Map<Integer, ColumnMetadata> columnMetadata, List<Range> readRanges)
    {
        return switch (field.getLogicalType()) {
            // FIXME: not sure if fieldId is same as the index in columnMetadata array
            case LogicalType.Int8Type _,
                 LogicalType.Int16Type _,
                 LogicalType.Int32Type _,
                 LogicalType.Int64Type _,
                 LogicalType.FloatType _,
                 LogicalType.DoubleType _,
                 LogicalType.StringType _,
                 LogicalType.BinaryType _ -> new PrimitiveColumnReader(dataSource, field, columnMetadata.get(field.getId()), readRanges);
//            case LogicalType.FixedSizeListType _ -> throw new RuntimeException("Unsupported logical type: " + field.getLogicalType());
            case LogicalType.ListType _ -> new ListColumnReader(dataSource, field, columnMetadata, readRanges);
            case LogicalType.StructType _ -> new StructColumnReader(dataSource, field, columnMetadata, readRanges);
            default -> throw new RuntimeException("Unsupported logical type: " + field.getLogicalType());
        };
    }

    void prepareNextRead(int batchSize);

    Block readBlock();

    DecodedPage read();
}
