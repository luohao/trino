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
package io.trino.lance.v2.metadata;

import com.lancedb.lance.protobuf.Encodings;

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
    private final List<DefinitionInterpretation> definitionInterpretations;
    // FIXME: add value encoding for FullZipLayout

    public FullZipLayout(int numRepBits,
            int numDeflBits,
            Block block,
            int numItems,
            int numVisibleItems,
            List<DefinitionInterpretation> definitionInterpretations)
    {
        this.numRepBits = requireNonNull(numRepBits, "numRepBits is null");
        this.numDeflBits = requireNonNull(numDeflBits, "numDeflBits is null");
        this.block = requireNonNull(block, "chunkSize is null");
        this.numItems = requireNonNull(numItems, "numItems is null");
        this.numVisibleItems = requireNonNull(numVisibleItems, "numVisibleItems is null");
        this.definitionInterpretations = requireNonNull(definitionInterpretations, "definitionInterpretations is null");
    }

    public static FullZipLayout fromProto(Encodings.FullZipLayout proto)
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
                DefinitionInterpretation.fromProtoList(proto.getLayersList()));
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
