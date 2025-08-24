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
import io.trino.lance.v2.metadata.DefinitionInterpretation;
import io.trino.lance.v2.reader.RepetitionDefinitionUnraveler.BlockPositions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.lance.v2.metadata.DefinitionInterpretation.ALL_VALID_ITEM;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.ALL_VALID_LIST;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.EMPTYABLE_LIST;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.NULLABLE_AND_EMPTYABLE_LIST;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.NULLABLE_ITEM;
import static io.trino.lance.v2.metadata.DefinitionInterpretation.NULLABLE_LIST;
import static org.assertj.core.api.Assertions.assertThat;

class TestRepDefUnravler
{
    @Test
    public void testBasicRepDef()
    {
        // [[I], [I, I]], NULL, [[NULL, NULL], NULL, [NULL, I, I, NULL]]
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {2, 1, 0, 2, 2, 0, 1, 1, 0, 0, 0},
                new int[] {0, 0, 0, 3, 1, 1, 2, 1, 0, 0, 1},
                new DefinitionInterpretation[] {NULLABLE_ITEM, NULLABLE_LIST, NULLABLE_LIST});

        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isPresent()).isTrue();
        assertThat(isNull.get()).isEqualTo(new boolean[] {false, false, false, true, true, true, false, false, true});

        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, false, true, false}), new int[] {0, 1, 3, 5, 5, 9}), innerPositions);
        BlockPositions outerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true, false}), new int[] {0, 2, 2, 5}), outerPositions);
    }

    @Test
    public void testEmptyListNoNull()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 0, 0, 1, 1, 1, 0}, new int[] {0, 0, 0, 0, 1, 1, 0, 0}, new DefinitionInterpretation[] {ALL_VALID_ITEM,
                EMPTYABLE_LIST});

        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions positions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, false, false}), new int[] {0, 4, 4, 4, 6}), positions);
    }

    @Test
    public void testNullList()
    {
        // nullable list
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 1, 1, 0, 0}, new int[] {0, 0, 2, 0, 1, 0}, new DefinitionInterpretation[] {NULLABLE_ITEM, NULLABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isPresent()).isTrue();
        assertThat(isNull.get()).isEqualTo(new boolean[] {false, false, false, true, false});
        BlockPositions positions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true, false}), new int[] {0, 2, 2, 5}), positions);
    }

    @Test
    public void testEmptyableList()
    {
        // emptyable list
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 1, 1, 0, 0}, new int[] {0, 0, 2, 0, 1, 0}, new DefinitionInterpretation[] {NULLABLE_ITEM, EMPTYABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isPresent()).isTrue();
        assertThat(isNull.get()).isEqualTo(new boolean[] {false, false, false, true, false});
        BlockPositions positions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, false}), new int[] {0, 2, 2, 5}), positions);
    }

    @Test
    public void testEmptyListAtEnd()
    {
        // last item is an empty list
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 1, 0, 0, 1}, new int[] {0, 0, 0, 1, 0, 2}, new DefinitionInterpretation[] {NULLABLE_ITEM, EMPTYABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isPresent()).isTrue();
        assertThat(isNull.get()).isEqualTo(new boolean[] {false, false, false, true, false});
        BlockPositions positions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, false}), new int[] {0, 2, 5, 5}), positions);
    }

    @Test
    public void testAllValid()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {2, 1, 0, 2, 0, 2, 0, 1, 0},
                null,
                new DefinitionInterpretation[] {ALL_VALID_ITEM, ALL_VALID_LIST, ALL_VALID_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.empty(), new int[] {0, 1, 3, 5, 7, 9}), innerPositions);
        BlockPositions outerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.empty(), new int[] {0, 2, 3, 5}), outerPositions);
    }

    @Test
    public void testOnlyEmptyLists()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 0, 0, 1, 1, 1, 0},
                new int[] {0, 0, 0, 0, 1, 1, 0, 0},
                new DefinitionInterpretation[] {ALL_VALID_ITEM, EMPTYABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, false, false}), new int[] {0, 4, 4, 4, 6}), innerPositions);
    }

    @Test
    public void testOnlyNullLists()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 0, 0, 1, 1, 1, 0},
                new int[] {0, 0, 0, 0, 1, 1, 0, 0},
                new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true, true, false}), new int[] {0, 4, 4, 4, 6}), innerPositions);
    }

    @Test
    public void testNullAndEmptyLists()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 0, 0, 0, 1, 1, 1, 0},
                new int[] {0, 0, 0, 0, 1, 2, 0, 0},
                new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_AND_EMPTYABLE_LIST});
        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true, false, false}), new int[] {0, 4, 4, 4, 6}), innerPositions);
    }

    @Test
    public void testOne()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 1, 1, 1, 1, 1, 1},
                new int[] {0, 0, 0, 0, 0, 1, 1},
                new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_AND_EMPTYABLE_LIST});
        Optional<boolean[]> nulls = unraveler.calculateNulls();
        BlockPositions positions = unraveler.calculateOffsets();
        return;
//        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true, false, false}), new int[] {0, 4, 4, 4, 6}), innerPositions);
    }

    @Test
    public void testNoRep()
    {
        SingleUnraveler unraveler = new SingleUnraveler(null, new int[] {2, 2, 0, 0, 1}, new DefinitionInterpretation[] {NULLABLE_ITEM, NULLABLE_ITEM, ALL_VALID_ITEM});
        Optional<boolean[]> innerNulls = unraveler.calculateNulls();
        assertThat(innerNulls.isPresent()).isTrue();
        assertThat(innerNulls.get()).isEqualTo(new boolean[] {true, true, false, false, true});
        Optional<boolean[]> middleNulls = unraveler.calculateNulls();
        assertThat(middleNulls.isPresent()).isTrue();
        assertThat(middleNulls.get()).isEqualTo(new boolean[] {true, true, false, false, false});
        Optional<boolean[]> outerNulls = unraveler.calculateNulls();
        assertThat(outerNulls.isEmpty()).isTrue();
    }

    @Test
    public void testNullsInStruct()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {1, 1, 1}, new int[] {1, 2, 1}, new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_LIST, NULLABLE_ITEM});
        Optional<boolean[]> innerNulls = unraveler.calculateNulls();
        assertThat(innerNulls.isEmpty()).isTrue();
        BlockPositions positions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {true, true, true}), new int[] {0, 0, 0, 0}), positions);
        Optional<boolean[]> outerNulls = unraveler.calculateNulls();
        assertThat(outerNulls.isPresent()).isTrue();
        assertThat(outerNulls.get()).isEqualTo(new boolean[] {false, true, false});
    }

    @Test
    public void testListEndsWithNull()
    {
        SingleUnraveler unraveler = new SingleUnraveler(new int[] {2, 2, 2}, new int[] {0, 1, 2}, new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_LIST, NULLABLE_LIST});
        Optional<boolean[]> innerNulls = unraveler.calculateNulls();
        assertThat(innerNulls.isEmpty()).isTrue();
        BlockPositions innerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, true}), new int[] {0, 1, 1}), innerPositions);
        BlockPositions outerPositions = unraveler.calculateOffsets();
        assertBlockPositionsEqual(new BlockPositions(Optional.of(new boolean[] {false, false, true}), new int[] {0, 1, 2, 2}), outerPositions);
    }

    @Test
    public void testCompositeUnravel()
    {
        CompositeUnraveler unraveler = new CompositeUnraveler(ImmutableList.of(
                new SingleUnraveler(new int[] {1, 0, 1, 1, 0, 0}, new int[] {0, 0, 1, 0, 0, 0}, new DefinitionInterpretation[] {ALL_VALID_ITEM, NULLABLE_LIST}),
                new SingleUnraveler(new int[] {1, 1, 0, 1, 0, 1, 0, 1, 0}, null, new DefinitionInterpretation[] {ALL_VALID_ITEM, ALL_VALID_LIST})));

        Optional<boolean[]> isNull = unraveler.calculateNulls();
        assertThat(isNull.isEmpty()).isTrue();
        BlockPositions positions = unraveler.calculateOffsets();
        assertThat(positions.offsets()).isEqualTo(new int[] {0, 2, 2, 5, 6, 8, 10, 12, 14});
        assertThat(positions.nulls().isPresent()).isTrue();
        assertThat(positions.nulls().get()).isEqualTo(new boolean[] {false, true, false, false, false, false, false, false});
    }

    public void assertBlockPositionsEqual(BlockPositions expected, BlockPositions actual)
    {
        assertThat(actual.nulls().isPresent()).isEqualTo(expected.nulls().isPresent());
        if (expected.nulls().isPresent()) {
            assertThat(actual.nulls().get()).isEqualTo(expected.nulls().get());
        }
        assertThat(actual.offsets()).isEqualTo(expected.offsets());
    }
}
