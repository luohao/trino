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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RepetitionIndex
{
    private RepetitionIndex() {}

    public static List<RepetitionIndex.RepIndexBlock> from(Slice slice, int depth)
    {
        checkArgument(depth > 0, "depth must be positive");
        long[][] repetitionIndex = new long[slice.length() / (depth + 1)][depth + 1];
        for (int i = 0; i < repetitionIndex.length; i++) {
            repetitionIndex[i] = slice.getLongs(i * 8 * (depth + 1), depth + 1);
        }
        boolean chunkHasPreamble = false;
        long offset = 0;
        ImmutableList.Builder<RepIndexBlock> builder = ImmutableList.builder();
        for (long[] chunkRepetitionIndex : repetitionIndex) {
            long endCount = chunkRepetitionIndex[0];
            long partialCount = chunkRepetitionIndex[1];
            boolean chunkHasTrailer = partialCount > 0;
            long startCount = endCount;
            if (chunkHasTrailer) {
                startCount++;
            }
            if (chunkHasPreamble) {
                startCount++;
            }
            builder.add(new RepIndexBlock(offset, startCount, chunkHasPreamble, chunkHasTrailer));
            chunkHasPreamble = chunkHasTrailer;
            offset += startCount;
        }
        return builder.build();
    }

    public static List<RepIndexBlock> defaultIndex(List<ChunkMetadata> chunks)
    {
        long offset = 0;
        ImmutableList.Builder<RepIndexBlock> builder = ImmutableList.builder();
        for (ChunkMetadata chunk : chunks) {
            long count = chunk.numValues();
            builder.add(new RepIndexBlock(offset, count, false, false));
            offset += count;
        }
        return builder.build();
    }

    // startCount is number of rows starts in this block(i.e., include trailer but not preamble)
    public record RepIndexBlock(long firstRow, long startCount, boolean hasPreamble, boolean hasTrailer) {}
}
