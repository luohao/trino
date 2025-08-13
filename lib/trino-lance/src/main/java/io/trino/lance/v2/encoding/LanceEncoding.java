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
package io.trino.lance.v2.encoding;

import com.lancedb.lance.protobuf.EncodingsV21;
import io.airlift.slice.Slice;
import io.trino.lance.v2.reader.BufferAdapter;
import io.trino.spi.block.ValueBlock;

import java.util.List;

import static java.lang.Math.toIntExact;

public interface LanceEncoding
{
    static LanceEncoding fromProto(EncodingsV21.CompressiveEncoding proto)
    {
        return switch (proto.getCompressionCase()) {
            case FLAT -> new FlatValueEncoding(toIntExact(proto.getFlat().getBitsPerValue() / Byte.SIZE));
            case INLINE_BITPACKING -> new InlineBitpackingEncoding(toIntExact(proto.getInlineBitpacking().getUncompressedBitsPerValue()));
            case VARIABLE -> new VariableEncoding();
            case FIXED_SIZE_LIST -> new FixedSizeListEncoding();
            default -> throw new IllegalArgumentException("Invalid encoding: " + proto.getCompressionCase());
        };
    }

    default ValueBlock decodeBlock(Slice slice, int count)
    {
        throw new UnsupportedOperationException("decodeBlock is not supported for " + getClass().getSimpleName());
    }

    default ValueBlock decodeMiniBlock(List<Slice> slices, int count)
    {
        throw new UnsupportedOperationException("decodeMiniBlock is not supported for " + getClass().getSimpleName());
    }

    <T> BufferAdapter<T> getBufferAdapter();

    <T> MiniBlockDecoder<T> getMiniBlockDecoder();
}
