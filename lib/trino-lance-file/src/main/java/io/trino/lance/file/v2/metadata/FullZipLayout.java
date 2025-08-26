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
package io.trino.lance.file.v2.metadata;

import com.lancedb.lance.protobuf.EncodingsV21;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class FullZipLayout
        implements PageLayout
{
    private final int numRepBits;
    private final int numDeflBits;
    private final Block block;
    private final int numItems;
    private final int numVisibleItems;
    private final List<RepDefLayer> repDefLayers;

    public FullZipLayout(int numRepBits,
            int numDeflBits,
            Block block,
            int numItems,
            int numVisibleItems,
            List<RepDefLayer> repDefLayers)
    {
        this.numRepBits = requireNonNull(numRepBits, "numRepBits is null");
        this.numDeflBits = requireNonNull(numDeflBits, "numDeflBits is null");
        this.block = requireNonNull(block, "chunkSize is null");
        this.numItems = requireNonNull(numItems, "numItems is null");
        this.numVisibleItems = requireNonNull(numVisibleItems, "numVisibleItems is null");
        this.repDefLayers = requireNonNull(repDefLayers, "repDefLayers is null");
    }

    public static FullZipLayout fromProto(EncodingsV21.FullZipLayout proto)
    {
        Block block = switch (proto.getDetailsCase()) {
            case BITS_PER_VALUE -> new Block.FixedWidthBlock(proto.getBitsPerValue());
            case BITS_PER_OFFSET -> new Block.VariableWidthBlock(proto.getBitsPerOffset());
            default -> throw new IllegalArgumentException("Unexpected details case: " + proto.getDetailsCase());
        };
        return new FullZipLayout(
                proto.getBitsRep(),
                proto.getBitsDef(),
                block,
                proto.getNumItems(),
                proto.getNumVisibleItems(),
                RepDefLayer.fromProtoList(proto.getLayersList()));
    }

    public sealed interface Block
            permits
            Block.FixedWidthBlock,
            Block.VariableWidthBlock
    {
        record FixedWidthBlock(int bitsPerValue)
                implements Block
        {
        }

        record VariableWidthBlock(int bitsPerOffset)
                implements Block
        {
        }
    }
}
