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
import io.trino.lance.LanceDataSource;
import io.trino.lance.v2.encoding.FlatValueEncoding;
import io.trino.lance.v2.encoding.LanceEncoding;
import io.trino.lance.v2.encoding.MiniBlockDecoder;
import io.trino.lance.v2.metadata.DefinitionInterpretation;
import io.trino.lance.v2.metadata.DiskRange;
import io.trino.lance.v2.metadata.MiniBlockLayout;
import io.trino.lance.v2.reader.RepetitionIndex.RepIndexBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.NULLABLE_ITEM;
import static io.trino.lance.v2.reader.IntArrayBufferAdapter.INT_ARRAY_BUFFER_ADAPTER;
import static java.lang.Math.toIntExact;

// map to DecodePageTask::decode()
public class MiniBlockPageReader
        implements PageReader
{
    public static final int MINIBLOCK_ALIGNMENT = 8;
    public static final int[] EMPTY_INT_ARRAY = new int[0];

    private final LanceDataSource dataSource;
    private final Type type;
    private final Optional<LanceEncoding> repetitionEncoding;
    private final Optional<LanceEncoding> definitionEncoding;
    private final Optional<LanceEncoding> dictionaryEncoding;
    private final Optional<Long> numDictionaryItems;
    private final Optional<Block> dictionaryBlock;
    private final LanceEncoding valueEncoding;
    private final int repetitionIndexDepth;
    private final List<RepIndexBlock> repetitionIndex;
    private final List<DefinitionInterpretation> defInterpretations;
    private final int maxVisibleDefinition;
    private final long numBuffers;
    private final List<DiskRange> bufferOffsets;
    // total number of rows in this page
    private final long numRows;
    private final List<ChunkMetadata> chunks;
    private final BufferAdapter valueBufferAdapter;
    private final DataValuesBuffer valuesBuffer;
    private final DataValuesBuffer<int[]> repetitionBuffer;
    private final DataValuesBuffer<int[]> definitionBuffer;
    private long levelOffset;

    public MiniBlockPageReader(LanceDataSource dataSource, Type type, MiniBlockLayout layout, List<DiskRange> bufferOffsets, long numRows)
    {
        this.dataSource = dataSource;
        this.type = type;
        this.repetitionEncoding = layout.repetitionEncoding();
        this.definitionEncoding = layout.definitionEncoding();
        this.dictionaryEncoding = layout.dictionaryEncoding();
        this.numDictionaryItems = layout.numDictionaryItems();
        this.valueEncoding = layout.valueEncoding();
        this.repetitionIndexDepth = layout.repIndexDepth();
        this.defInterpretations = layout.interpretations();
        this.maxVisibleDefinition = defInterpretations.stream().takeWhile(interpretation -> !interpretation.isList()).mapToInt(DefinitionInterpretation::numDefLevels).sum();
        this.numBuffers = layout.numBuffers();
        this.bufferOffsets = bufferOffsets;
        this.numRows = numRows;
        try {
            // build chunk meta
            // bufferOffsets[0] = chunk metadata buffer
            // bufferOffset[1] = value buffer
            DiskRange chunkMetadataBuf = bufferOffsets.get(0);
            DiskRange valueBuf = bufferOffsets.get(1);
            Slice chunkMetadataSlice = dataSource.readFully(chunkMetadataBuf.getPosition(), toIntExact(chunkMetadataBuf.getLength()));
            int numWords = chunkMetadataSlice.length() / 2;
            ImmutableList.Builder<ChunkMetadata> chunkMetadataBuilder = ImmutableList.builder();
            long count = 0;
            long offset = valueBuf.getPosition();
            for (int i = 0; i < numWords; i++) {
                int word = chunkMetadataSlice.getUnsignedShort(i * 2);
                int logNumValues = word & 0xF;
                int dividedBytes = word >>> 4;
                int chunkSizeBytes = (dividedBytes + 1) * MINIBLOCK_ALIGNMENT;
                long numValues = i < numWords - 1 ? 1 << logNumValues : numRows - count;
                count += numValues;

                chunkMetadataBuilder.add(new ChunkMetadata(numValues, chunkSizeBytes, offset));
                offset += chunkSizeBytes;
            }
            this.chunks = chunkMetadataBuilder.build();
            // load dictionary
            if (dictionaryEncoding.isPresent()) {
                DiskRange dictionaryRange = bufferOffsets.get(2);
                Slice dictionarySlice = dataSource.readFully(dictionaryRange.getPosition(), toIntExact(dictionaryRange.getLength()));
                ValueBlock dictionary = dictionaryEncoding.get().decodeBlock(dictionarySlice, toIntExact(numDictionaryItems.get()));
                // if a block is nullable, we append null to the end of dictionary
                if (defInterpretations.stream().anyMatch(interpretation -> (interpretation == NULLABLE_ITEM))) {
                    dictionaryBlock = Optional.of(dictionary.copyWithAppendedNull());
                }
                else {
                    dictionaryBlock = Optional.of(dictionary);
                }
            }
            else {
                dictionaryBlock = Optional.empty();
            }
            // load rep index
            if (repetitionIndexDepth > 0) {
                // TODO: refactor for readability
                DiskRange repetitionIndexRange = bufferOffsets.getLast();
                verify(repetitionIndexRange.getLength() % 8 == 0);
                Slice repetitionIndexSlice = dataSource.readFully(repetitionIndexRange.getPosition(), toIntExact(repetitionIndexRange.getLength()));
                repetitionIndex = RepetitionIndex.from(repetitionIndexSlice, repetitionIndexDepth);
            }
            else {
                repetitionIndex = RepetitionIndex.defaultIndex(chunks);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        valueBufferAdapter = valueEncoding.getBufferAdapter();
        valuesBuffer = new DataValuesBuffer(valueBufferAdapter);
        repetitionBuffer = new DataValuesBuffer<>(INT_ARRAY_BUFFER_ADAPTER);
        definitionBuffer = new DataValuesBuffer<>(INT_ARRAY_BUFFER_ADAPTER);
        levelOffset = 0;
    }

    public static final int padding(int n)
    {
        return (MINIBLOCK_ALIGNMENT - (n & (MINIBLOCK_ALIGNMENT - 1))) & (MINIBLOCK_ALIGNMENT - 1);
    }

    @Override
    public Block readRanges(List<Range> ranges)
    {
        // FIXME: maybe create temp buffer within the scope of readRanges()?
        valuesBuffer.reset();
        repetitionBuffer.reset();
        definitionBuffer.reset();
        levelOffset = 0;

        for (Range range : ranges) {
            long rowsNeeded = range.length();
            boolean needPreamble = false;

            // find first chunk that has row >= range.start
            int blockIndex = Collections.binarySearch(repetitionIndex, range.start(), (block, key) -> Long.compare(((RepIndexBlock) block).firstRow(), (long) key));
            if (blockIndex >= 0) {
                while (blockIndex > 0 && repetitionIndex.get(blockIndex - 1).firstRow() == range.start()) {
                    blockIndex--;
                }
            }
            else {
                blockIndex = -(blockIndex + 1) - 1;
            }

            long toSkip = range.start() - repetitionIndex.get(blockIndex).firstRow();
            while (rowsNeeded > 0 || needPreamble) {
                RepIndexBlock chunkIndexBlock = repetitionIndex.get(blockIndex);
                long rowsAvailable = chunkIndexBlock.startCount() - toSkip;
                verify(rowsAvailable > 0);

                long rowsToTake = Math.min(rowsNeeded, rowsAvailable);
                rowsNeeded -= rowsToTake;

                boolean takeTrailer = false;
                long fullRowsToTake = rowsToTake;

                if (rowsToTake == rowsAvailable && chunkIndexBlock.hasTrailer()) {
                    takeTrailer = true;
                    needPreamble = true;
                    fullRowsToTake--;
                }
                else {
                    needPreamble = false;
                }
                // FIXME: loadChunk()
                // FIXME: merge chunks
                // ChunkData maps to DecodedMiniBlockChunk
                readChunk(chunks.get(blockIndex), chunkIndexBlock, toSkip, rowsToTake, needPreamble, takeTrailer);

                // TODO: append the data to the output buffer
                toSkip = 0;
                blockIndex++;
            }
        }

        return valuesBuffer.createBlock(definitionBuffer.getMergedValues(), repetitionBuffer.getMergedValues(), dictionaryBlock);
    }

    @Override
    public DecodedPage decodeRanges(List<Range> ranges)
    {
        // FIXME: maybe create temp buffer within the scope of readRanges()?
        valuesBuffer.reset();
        repetitionBuffer.reset();
        definitionBuffer.reset();
        levelOffset = 0;

        for (Range range : ranges) {
            long rowsNeeded = range.length();
            boolean needPreamble = false;

            // find first chunk that has row >= range.start
            int blockIndex = Collections.binarySearch(repetitionIndex, range.start(), (block, key) -> Long.compare(((RepIndexBlock) block).firstRow(), (long) key));
            if (blockIndex >= 0) {
                while (blockIndex > 0 && repetitionIndex.get(blockIndex - 1).firstRow() == range.start()) {
                    blockIndex--;
                }
            }
            else {
                blockIndex = -(blockIndex + 1) - 1;
            }

            long toSkip = range.start() - repetitionIndex.get(blockIndex).firstRow();
            while (rowsNeeded > 0 || needPreamble) {
                RepIndexBlock chunkIndexBlock = repetitionIndex.get(blockIndex);
                long rowsAvailable = chunkIndexBlock.startCount() - toSkip;
                verify(rowsAvailable > 0);

                long rowsToTake = Math.min(rowsNeeded, rowsAvailable);
                rowsNeeded -= rowsToTake;

                boolean takeTrailer = false;
                long fullRowsToTake = rowsToTake;

                if (rowsToTake == rowsAvailable && chunkIndexBlock.hasTrailer()) {
                    takeTrailer = true;
                    needPreamble = true;
                    fullRowsToTake--;
                }
                else {
                    needPreamble = false;
                }
                // FIXME: loadChunk()
                // FIXME: merge chunks
                // ChunkData maps to DecodedMiniBlockChunk
                readChunk(chunks.get(blockIndex), chunkIndexBlock, toSkip, rowsToTake, needPreamble, takeTrailer);

                // TODO: append the data to the output buffer
                toSkip = 0;
                blockIndex++;
            }
        }
        return valuesBuffer.createDecodedPage(definitionBuffer.getMergedValues(), repetitionBuffer.getMergedValues(), defInterpretations, dictionaryBlock);
    }

    public void readChunk(ChunkMetadata chunk, RepIndexBlock chunkIndex, long rowsToSkip, long rowsToTake, boolean needPreamble, boolean takeTrailer)
    {
        // 1. decode the whole miniblock chunk
        // 2. find the row range start and end from skip/take
        // 3. use the rep ino to map the row range to an item range
        // 4. append the data
        try {
            long maxRepetitionLevel = defInterpretations.stream().filter(DefinitionInterpretation::isList).count();
            ChunkReader chunkReader = new ChunkReader(dataSource.readFully(chunk.offsetBytes(), toIntExact(chunk.chunkSizeBytes())), toIntExact(chunk.numValues()), valueBufferAdapter);
            long rowRangeStart = rowsToSkip;
            long rowRangeEnd = rowsToSkip + rowsToTake;
            Range itemRange;
            Range levelRange;
            // TODO: full map_ranges
            if (repetitionEncoding.isPresent()) {
                int[] rep = chunkReader.readRepetitionLevels();
                int itemsInPreamble = 0;
                int firstRowStart = -1;

                if (definitionEncoding.isPresent()) {
                    int[] def = chunkReader.readDefinitionLevels();
                    for (int i = 0; i < rep.length; i++) {
                        if (rep[i] == maxRepetitionLevel) {
                            firstRowStart = i;
                            break;
                        }
                        if (def[i] <= maxVisibleDefinition) {
                            itemsInPreamble++;
                        }
                    }
                }
                else {
                    for (int i = 0; i < rep.length; i++) {
                        if (rep[i] == maxRepetitionLevel) {
                            firstRowStart = i;
                            break;
                        }
                    }
                }
                // FIXME: handle cases where a chunk is entirely partial values
                verify(firstRowStart != -1);
                rep = Arrays.copyOfRange(rep, firstRowStart, rep.length);

                // FIXME: handle preamble
                verify(rowRangeStart < rowRangeEnd);

                int rowsSeen = 0;
                int newStart = 0;
                int newLevelsStart = 0;
                if (definitionEncoding.isPresent()) {
                    int[] def = Arrays.copyOfRange(chunkReader.readDefinitionLevels(), firstRowStart, chunkReader.readDefinitionLevels().length - 1);
                    long leadInvisSeen = 0;
                    if (rowRangeStart > 0) {
                        if (def[0] > maxVisibleDefinition) {
                            leadInvisSeen += 1;
                        }
                        for (int i = 1; i < def.length; i++) {
                            if (rep[i] == maxRepetitionLevel) {
                                rowsSeen++;
                                if (rowsSeen == rowRangeStart) {
                                    newStart = i + 1 - toIntExact(leadInvisSeen);
                                    newLevelsStart = i + 1;
                                    break;
                                }
                                if (def[i] > maxVisibleDefinition) {
                                    leadInvisSeen++;
                                }
                            }
                        }
                    }
                    rowsSeen++;
                    long newEnd = Long.MAX_VALUE;
                    long newLevelsEnd = rep.length;
                    boolean newStartIsInvisible = def[newLevelsStart] <= maxVisibleDefinition;
                    long trailInvisSeen = newStartIsInvisible ? 0 : 1;

                    for (int i = newLevelsStart + 1; i < rep.length; i++) {
                        int repLevel = rep[i];
                        int defLevel = def[i];
                        if (repLevel == maxRepetitionLevel) {
                            rowsSeen++;
                            if (rowsSeen == rowRangeEnd + 1) {
                                newEnd = i - trailInvisSeen;
                                newLevelsEnd = i;
                                break;
                            }
                            if (defLevel > maxVisibleDefinition) {
                                trailInvisSeen++;
                            }
                        }
                    }
                    if (newEnd == Long.MAX_VALUE) {
                        newLevelsEnd = rep.length;
                        newEnd = rep.length - (leadInvisSeen + trailInvisSeen);
                    }
                    verify(newEnd != Long.MAX_VALUE);
                    if (needPreamble) {
                        newEnd += firstRowStart;
                        newLevelsEnd += firstRowStart;
                    }
                    else {
                        newStart += firstRowStart;
                        newEnd += firstRowStart;
                        newLevelsStart += firstRowStart;
                        newLevelsEnd += firstRowStart;
                    }
                    itemRange = new Range(newStart, newEnd);
                    levelRange = new Range(newLevelsStart, newLevelsEnd);
                }
                else {
                    if (rowRangeStart > 0) {
                        for (int i = 1; i < rep.length; i++) {
                            if (rep[i] == maxRepetitionLevel) {
                                rowsSeen++;
                                if (rowsSeen == rowRangeStart) {
                                    newStart = i + 1;
                                    break;
                                }
                            }
                        }
                    }
                    long end = rep.length;
                    if (rowRangeEnd < chunk.numValues()) {
                        for (int i = newStart; i < rep.length; i++) {
                            if (rep[i] == toIntExact(maxRepetitionLevel)) {
                                rowsSeen++;
                                if (rowsSeen == rowRangeEnd) {
                                    end = i + 1;
                                    break;
                                }
                            }
                        }
                    }
                    if (needPreamble) {
                        end += firstRowStart;
                    }
                    else {
                        newStart += firstRowStart;
                        end += firstRowStart;
                    }
                    itemRange = new Range(newStart, end);
                    levelRange = new Range(newStart, end);
                }
            }
            else {
                itemRange = new Range(rowRangeStart, rowRangeEnd);
                levelRange = itemRange.clone();
            }

            // extend_levels
            if (!repetitionEncoding.isEmpty()) {
                if (repetitionBuffer.isEmpty()) {
                    repetitionBuffer.append(new int[toIntExact(levelOffset)]);
                }
                repetitionBuffer.append(Arrays.copyOfRange(chunkReader.readRepetitionLevels(), toIntExact(levelRange.start()), toIntExact(levelRange.length())));
            }
//            else {
//                // TODO: FIXME
//                throw new UnsupportedOperationException();
//            }

            if (!definitionEncoding.isEmpty()) {
                if (definitionBuffer.isEmpty() && levelOffset > 0) {
                    definitionBuffer.append(new int[toIntExact(levelOffset)]);
                }
                definitionBuffer.append(Arrays.copyOfRange(chunkReader.readDefinitionLevels(), toIntExact(levelRange.start()), toIntExact(levelRange.length())));
            }
//            else {
//                // TODO: FIXME
//                throw new UnsupportedOperationException();
//            }

            levelOffset += levelRange.length();
            chunkReader.readValues(toIntExact(itemRange.start()), toIntExact(itemRange.length()));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int[] loadRepetitionLevels(Slice slice)
    {
        checkArgument(repetitionEncoding.isPresent());
        checkArgument(repetitionEncoding.get() instanceof FlatValueEncoding);
        checkArgument(((FlatValueEncoding) repetitionEncoding.get()).getBytesPerValue() == 2);
        return readUnsignedShorts(slice);
    }

    private int[] loadDefinitionLevels(Slice slice)
    {
        checkArgument(definitionEncoding.isPresent());
        checkArgument(definitionEncoding.get() instanceof FlatValueEncoding);
        checkArgument(((FlatValueEncoding) definitionEncoding.get()).getBytesPerValue() == 2);
        return readUnsignedShorts(slice);
    }

    private int[] readUnsignedShorts(Slice slice)
    {
        int[] data = new int[slice.length() / 2];
        for (int i = 0; i < slice.length() / 2; i++) {
            data[i] = slice.getUnsignedShort(i * 2);
        }
        return data;
    }

    public class ChunkReader<T>
    {
        private final BufferAdapter<T> bufferAdapter;
        private final List<Slice> buffers;
        private final MiniBlockDecoder<T> valueDecoder;
        private final int[] repetitions;
        private final int[] definitions;

        public ChunkReader(Slice chunk, int numValues, BufferAdapter<T> bufferAdapter)
        {
            this.bufferAdapter = bufferAdapter;
            // decode header
            int offset = 0;
            // FIXME: whats this used
            int numLevels = chunk.getUnsignedShort(offset);
            offset += 2;

            int repetitionSize = 0;
            if (repetitionEncoding.isPresent()) {
                repetitionSize = toIntExact(chunk.getUnsignedShort(offset));
                offset += 2;
            }

            int definitionSize = 0;
            if (definitionEncoding.isPresent()) {
                definitionSize = toIntExact(chunk.getUnsignedShort(offset));
                offset += 2;
            }

            int[] bufferSizes = new int[toIntExact(numBuffers)];
            for (int i = 0; i < numBuffers; i++) {
                bufferSizes[i] = chunk.getUnsignedShort(offset + i * 2);
                offset += 2;
            }
            offset += padding(offset);

            // load rep/def
            if (repetitionEncoding.isPresent()) {
                repetitions = loadRepetitionLevels(chunk.slice(offset, repetitionSize));
                offset += repetitionSize;
                offset += padding(offset);
            }
            else {
                repetitions = EMPTY_INT_ARRAY;
            }
            if (definitionEncoding.isPresent()) {
                definitions = loadDefinitionLevels(chunk.slice(offset, definitionSize));
                offset += definitionSize;
                offset += padding(offset);
            }
            else {
                definitions = EMPTY_INT_ARRAY;
            }

            // load data buffers
            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            for (int i = 0; i < bufferSizes.length; i++) {
                int size = bufferSizes[i];
                builder.add(chunk.slice(offset, size));
                offset += size;
                offset += padding(offset);
            }
            buffers = builder.build();
            valueDecoder = valueEncoding.getMiniBlockDecoder();
            valueDecoder.init(buffers, numValues);
        }

        public void readValues(int offset, int count)
        {
            T output = bufferAdapter.createBuffer(count);
            valueDecoder.read(offset, output, 0, count);
            valuesBuffer.append(output);
        }

        public int[] readDefinitionLevels()
        {
            return definitions;
        }

        public int[] readRepetitionLevels()
        {
            return repetitions;
        }
    }
}
