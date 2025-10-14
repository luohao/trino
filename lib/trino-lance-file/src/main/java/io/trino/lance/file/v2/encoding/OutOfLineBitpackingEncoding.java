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
package io.trino.lance.file.v2.encoding;

import build.buf.gen.lance.encodings21.OutOfLineBitpacking;
import com.github.luohao.fastlanes.bitpack.VectorBytePacker;
import com.github.luohao.fastlanes.bitpack.VectorIntegerPacker;
import com.github.luohao.fastlanes.bitpack.VectorLongPacker;
import com.github.luohao.fastlanes.bitpack.VectorShortPacker;
import io.airlift.slice.Slice;
import io.trino.lance.file.v2.reader.BufferAdapter;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ValueBlock;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OutOfLineBitpackingEncoding
        implements LanceEncoding
{
    public static final int ELEMENTS_PER_CHUNK = 1024;
    private final int uncompressedBitsPerValue;
    private final int compressedBitsPerValue;
    private final int chunkSize;

    public OutOfLineBitpackingEncoding(int uncompressedBitsPerValue, int compressedBitsPerValue)
    {
        this.uncompressedBitsPerValue = uncompressedBitsPerValue;
        this.compressedBitsPerValue = compressedBitsPerValue;
        this.chunkSize = (ELEMENTS_PER_CHUNK * compressedBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        throw new UnsupportedOperationException("getBufferAdapter not supported for OutOfLineBitpacking");
    }

    @Override
    public <T> MiniBlockDecoder<T> getMiniBlockDecoder()
    {
        throw new UnsupportedOperationException("getMiniBlockDecoder not supported for OutOfLineBitpacking");
    }

    public static class BitUnpacker
    {
        private final Slice slice;
        private final int bitsPerValue;

        private int byteOffset;
        private int currentByte;
        private int bitsLeft;

        public BitUnpacker(Slice slice, int bitsPerValue)
        {
            this.slice = requireNonNull(slice, "slice is null");
            this.bitsPerValue = bitsPerValue;
        }

        public long unpackOne()
        {
            long result = 0;
            int bitsToRead = bitsPerValue;

            while (bitsToRead > bitsLeft) {
                // Use up the remaining bits in currentByte
                result <<= bitsLeft;
                result |= currentByte & ((1L << bitsLeft) - 1);
                bitsToRead -= bitsLeft;

                currentByte = slice.getByte(byteOffset) & 0xFF;
                byteOffset++;
                bitsLeft = Byte.SIZE;
            }

            if (bitsToRead > 0) {
                result <<= bitsToRead;
                bitsLeft -= bitsToRead;
                result |= (currentByte >> bitsLeft) & ((1L << bitsToRead) - 1);
            }

            return result;
        }
    }

    public int getUncompressedBitsPerValue()
    {
        return uncompressedBitsPerValue;
    }

    public int getCompressedBitsPerValue()
    {
        return compressedBitsPerValue;
    }

    public byte[] unpackBytes(Slice slice, int count)
    {
        byte[] values = new byte[count];
        int numFullChunks = count / ELEMENTS_PER_CHUNK;
        int numTailValues = count % ELEMENTS_PER_CHUNK;
        int currentOffset = 0;
        byte[] buffer = new byte[ELEMENTS_PER_CHUNK];

        for (int chunk = 0; chunk < numFullChunks; chunk++) {
            Slice chunkSlice = slice.slice(chunkSize * chunk, chunkSize);
            VectorBytePacker.unpack(chunkSlice.getBytes(0, chunkSize), compressedBitsPerValue, buffer);
            System.arraycopy(buffer, 0, values, currentOffset, ELEMENTS_PER_CHUNK);
            currentOffset += ELEMENTS_PER_CHUNK;
        }

        if (numTailValues > 0) {
            int tailByteOffset = chunkSize * numFullChunks;
            int tailSize = slice.length() - tailByteOffset;
            Slice tailSlice = slice.slice(tailByteOffset, tailSize);
            if (tailSize * Byte.SIZE == numTailValues * uncompressedBitsPerValue) {
                tailSlice.getBytes(0, values, currentOffset, numTailValues);
            }
            else {
                checkArgument(tailSize == chunkSize, "tail chunk size must be equal to full chunk size if bitpacked");
                VectorBytePacker.unpack(tailSlice.getBytes(), compressedBitsPerValue, buffer);
                System.arraycopy(buffer, 0, values, currentOffset, numTailValues);
            }
        }
        return values;
    }

    public short[] unpackShorts(Slice slice, int count)
    {
        short[] values = new short[count];
        int numFullChunks = count / ELEMENTS_PER_CHUNK;
        int numTailValues = count % ELEMENTS_PER_CHUNK;
        int currentOffset = 0;
        short[] buffer = new short[ELEMENTS_PER_CHUNK];

        for (int chunk = 0; chunk < numFullChunks; chunk++) {
            Slice chunkSlice = slice.slice(chunkSize * chunk, chunkSize);
            VectorShortPacker.unpack(chunkSlice.getShorts(0, chunkSize / Short.BYTES), compressedBitsPerValue, buffer);
            System.arraycopy(buffer, 0, values, currentOffset, ELEMENTS_PER_CHUNK);
            currentOffset += ELEMENTS_PER_CHUNK;
        }
        if (numTailValues > 0) {
            int tailByteOffset = chunkSize * numFullChunks;
            int tailSize = slice.length() - tailByteOffset;
            Slice tailSlice = slice.slice(tailByteOffset, tailSize);
            if (tailSize * Byte.SIZE == numTailValues * uncompressedBitsPerValue) {
                tailSlice.getShorts(0, values, currentOffset, numTailValues);
            }
            else {
                checkArgument(tailSize == chunkSize, "tail chunk size must be equal to full chunk size if bitpacked");
                VectorShortPacker.unpack(tailSlice.getShorts(0, chunkSize / Short.BYTES), compressedBitsPerValue, buffer);
                System.arraycopy(buffer, 0, values, currentOffset, numTailValues);
            }
        }
        return values;
    }

    public int[] unpackInts(Slice slice, int count)
    {
        int[] values = new int[count];
        int numFullChunks = count / ELEMENTS_PER_CHUNK;
        int numTailValues = count % ELEMENTS_PER_CHUNK;
        int currentOffset = 0;
        int[] buffer = new int[ELEMENTS_PER_CHUNK];

        for (int chunk = 0; chunk < numFullChunks; chunk++) {
            Slice chunkSlice = slice.slice(chunkSize * chunk, chunkSize);
            VectorIntegerPacker.unpack(chunkSlice.getInts(0, chunkSize / Integer.BYTES), compressedBitsPerValue, buffer);
            System.arraycopy(buffer, 0, values, currentOffset, ELEMENTS_PER_CHUNK);
            currentOffset += ELEMENTS_PER_CHUNK;
        }
        if (numTailValues > 0) {
            int tailByteOffset = chunkSize * numFullChunks;
            int tailSize = slice.length() - tailByteOffset;
            Slice tailSlice = slice.slice(tailByteOffset, tailSize);
            if (tailSize * Byte.SIZE == numTailValues * uncompressedBitsPerValue) {
                tailSlice.getInts(0, values, currentOffset, numTailValues);
            }
            else {
                checkArgument(tailSize == chunkSize, "tail chunk size must be equal to full chunk size if bitpacked");
                VectorIntegerPacker.unpack(tailSlice.getInts(0, chunkSize / Integer.BYTES), compressedBitsPerValue, buffer);
                System.arraycopy(buffer, 0, values, currentOffset, numTailValues);
            }
        }
        return values;
    }

    public long[] unpackLongs(Slice slice, int count)
    {
        long[] values = new long[count];
        int numFullChunks = count / ELEMENTS_PER_CHUNK;
        int numTailValues = count % ELEMENTS_PER_CHUNK;
        int currentOffset = 0;
        long[] buffer = new long[ELEMENTS_PER_CHUNK];

        for (int chunk = 0; chunk < numFullChunks; chunk++) {
            Slice chunkSlice = slice.slice(chunkSize * chunk, chunkSize);
            VectorLongPacker.unpack(chunkSlice.getLongs(0, chunkSize / Long.BYTES), compressedBitsPerValue, buffer);
            System.arraycopy(buffer, 0, values, currentOffset, ELEMENTS_PER_CHUNK);
            currentOffset += ELEMENTS_PER_CHUNK;
        }
        if (numTailValues > 0) {
            int tailByteOffset = chunkSize * numFullChunks;
            int tailSize = slice.length() - tailByteOffset;
            Slice tailSlice = slice.slice(tailByteOffset, tailSize);
            if (tailSize * Byte.SIZE == numTailValues * uncompressedBitsPerValue) {
                tailSlice.getLongs(0, values, currentOffset, numTailValues);
            }
            else {
                checkArgument(tailSize == chunkSize, "tail chunk size must be equal to full chunk size if bitpacked");
                VectorLongPacker.unpack(tailSlice.getLongs(0, chunkSize / Long.BYTES), compressedBitsPerValue, buffer);
                System.arraycopy(buffer, 0, values, currentOffset, numTailValues);
            }
        }
        return values;
    }

    @Override
    public ValueBlock decodeBlock(Slice slice, int count)
    {
        checkArgument(count > 0, "count must be positive");

        switch (uncompressedBitsPerValue) {
            case 8:
                byte[] bytes = unpackBytes(slice, count);
                return new ByteArrayBlock(count, Optional.empty(), bytes);
            case 16:
                short[] shorts = unpackShorts(slice, count);
                return new ShortArrayBlock(count, Optional.empty(), shorts);
            case 32:
                int[] ints = unpackInts(slice, count);
                return new IntArrayBlock(count, Optional.empty(), ints);
            case 64:
                long[] longs = unpackLongs(slice, count);
                return new LongArrayBlock(count, Optional.empty(), longs);
            default:
                throw new IllegalStateException("Unexpected uncompressedBitWidth: " + uncompressedBitsPerValue);
        }
    }

    public static OutOfLineBitpackingEncoding fromProto(OutOfLineBitpacking proto)
    {
        checkArgument(proto.hasValues());
        checkArgument(proto.getValues().hasFlat());
        int uncompressedBitsPerValue = toIntExact(proto.getUncompressedBitsPerValue());
        int compressedBitsPerValue = toIntExact(proto.getValues().getFlat().getBitsPerValue());
        return new OutOfLineBitpackingEncoding(uncompressedBitsPerValue, compressedBitsPerValue);
    }
}
