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
package io.trino.lance.v2;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.lancedb.lance.file.LanceFileWriter;
import io.trino.lance.FileLanceDataSource;
import io.trino.lance.LanceDataSource;
import io.trino.lance.LanceReader;
import io.trino.lance.TempFile;
import io.trino.lance.v2.metadata.LogicalType;
import io.trino.lance.v2.metadata.TypeUtil;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class LanceTester
{
    public static final ArrowType ARROW_INT8_TYPE = new ArrowType.Int(8, false);
    public static final ArrowType ARROW_INT16_TYPE = new ArrowType.Int(16, false);
    public static final ArrowType ARROW_INT32_TYPE = new ArrowType.Int(32, false);
    public static final ArrowType ARROW_INT64_TYPE = new ArrowType.Int(64, false);
    public static final ArrowType ARROW_REAL_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    public static final ArrowType ARROW_DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    public static final ArrowType ARROW_STRING_TYPE = new ArrowType.Utf8();
    public static final ArrowType ARROW_BINARY_TYPE = new ArrowType.Binary();
    public static final long MAX_LIST_SIZE = 1000;

    private static final Map<Class<? extends Type>, BiConsumer<FieldVectorContext, List<?>>> typedWriters = Map.of(TinyintType.class, (context, values) -> writeTyped((TinyIntVector) context.vector, values, Byte.class, (i, value) -> ((TinyIntVector) context.vector).setSafe(i, value)), SmallintType.class, (context, values) -> writeTyped((SmallIntVector) context.vector, values, Short.class, (i, value) -> ((SmallIntVector) context.vector).setSafe(i, value)), IntegerType.class, (context, values) -> writeTyped((IntVector) context.vector, values, Integer.class, (i, value) -> ((IntVector) context.vector).setSafe(i, value)), BigintType.class, (context, values) -> writeTyped((BigIntVector) context.vector, values, Long.class, (i, value) -> ((BigIntVector) context.vector).setSafe(i, value)), RealType.class, (context, values) -> writeTyped((Float4Vector) context.vector, values, Float.class, (i, value) -> ((Float4Vector) context.vector).setSafe(i, value)), DoubleType.class, (context, values) -> writeTyped((Float8Vector) context.vector, values, Double.class, (i, value) -> ((Float8Vector) context.vector).setSafe(i, value)), VarcharType.class, (context, values) -> writeTyped((VarCharVector) context.vector, values, String.class, (i, value) -> ((VarCharVector) context.vector).setSafe(i, value.getBytes(StandardCharsets.UTF_8))), VarbinaryType.class, (context, values) -> writeTyped((VarBinaryVector) context.vector, values, byte[].class, (i, value) -> ((VarBinaryVector) context.vector).setSafe(i, value)));
    private static final Random random = new Random();

    private static void assertFileContentsTrino(Type type, TempFile tempFile, List<?> expectedValues)
            throws IOException
    {
        try (LanceReader lanceReader = createLanceReader(tempFile, type)) {
            Iterator<?> iterator = expectedValues.iterator();

            int rowsProcessed = 0;
            for (SourcePage page = lanceReader.nextSourcePage(); page != null; page = lanceReader.nextSourcePage()) {
                int batchSize = page.getPositionCount();
                Block block = page.getBlock(0);
                List<Object> data = new ArrayList<>(block.getPositionCount());
                for (int position = 0; position < block.getPositionCount(); position++) {
                    data.add(type.getObjectValue(block, position));
                }
                for (int i = 0; i < batchSize; i++) {
                    assertThat(iterator.hasNext()).isTrue();
                    Object expected = iterator.next();
                    Object actual = data.get(i);
//                    System.out.println(i + ": " + actual);
                    assertColumnValueEquals(type, actual, expected);
                }
                rowsProcessed += batchSize;
            }
            assertThat(iterator.hasNext()).isFalse();
            assertThat(lanceReader.nextSourcePage()).isNull();
            assertThat(rowsProcessed).isEqualTo(expectedValues.size());
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertThat(expected).isNull();
            return;
        }

        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertThat(actualRow).hasSize(fieldTypes.size());
            assertThat(actualRow).hasSize(expectedRow.size());
            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static LanceReader createLanceReader(TempFile tempFile, Type type)
            throws IOException
    {
        LanceDataSource dataSource = new FileLanceDataSource(tempFile.getFile());
        return new LanceReader(dataSource, ImmutableList.of(0), Optional.empty());
    }

    public static void writeLancePrimitiveColumnJNI(File outputFile, Type type, List<?> values, boolean nullable)
            throws Exception
    {
        BufferAllocator allocator = new RootAllocator();
        LanceFileWriter writer = LanceFileWriter.open(outputFile.getPath(), allocator, null);
        String columnName = type.getDisplayName();
        Field field = nullable ? Field.nullable(columnName, toArrowPrimitiveType(type)) : Field.notNullable(type.getDisplayName(), toArrowPrimitiveType(type));
        Schema schema = new Schema(ImmutableList.of(field), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        FieldVector vector = root.getVector(columnName);
        writeFieldVector(vector, type, values);
        root.setRowCount(values.size());
        writer.write(root);
        writer.close();
    }

    public static void writeLanceColumnJNI(File outputFile, Type type, List<?> values, boolean nullable)
            throws Exception
    {
        BufferAllocator allocator = new RootAllocator();
        LanceFileWriter writer = LanceFileWriter.open(outputFile.getPath(), allocator, null);
        String columnName = type.getDisplayName();
        Field field = toArrowField(columnName, type, nullable);
        Schema schema = new Schema(ImmutableList.of(field), null);
        List<List<?>> data = values.stream().map(Arrays::asList).collect(toImmutableList());
        VectorSchemaRoot root = populateVectorSchemaRoot(schema, data, allocator);
        writer.write(root);
        writer.close();
    }

    private static ArrowType toArrowPrimitiveType(Type type)
    {
        return switch (type) {
            case TinyintType _ -> new ArrowType.Int(8, true);
            case SmallintType _ -> new ArrowType.Int(16, true);
            case IntegerType _ -> new ArrowType.Int(32, true);
            case BigintType _ -> new ArrowType.Int(64, true);
            case RealType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DoubleType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VarcharType _ -> new ArrowType.Utf8();
            case VarbinaryType _ -> new ArrowType.Binary();
            default -> throw new UnsupportedOperationException();
        };
    }

    public static Field toArrowField(String name, Type type, boolean nullable)
    {
        return switch (type) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _, RealType _, DoubleType _, VarcharType _, VarbinaryType _ ->
                    new Field(name, nullable ? FieldType.nullable(toArrowPrimitiveType(type)) : FieldType.notNullable(toArrowPrimitiveType(type)), ImmutableList.of());
            case RowType row -> {
                List<Field> childFields = row.getFields().stream().map(child -> toArrowField(child.getName().orElse(""), child.getType(), nullable)).collect(toImmutableList());
                yield new Field(name, nullable ? FieldType.nullable(ArrowType.Struct.INSTANCE) : FieldType.notNullable(ArrowType.Struct.INSTANCE), childFields);
            }
            case ArrayType array -> {
                Field element = toArrowField("element", array.getElementType(), true);
                yield new Field(name, nullable ? FieldType.nullable(ArrowType.List.INSTANCE) : FieldType.notNullable(ArrowType.List.INSTANCE), ImmutableList.of(element));
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    private static <T, V extends FieldVector> void writeTyped(V vector, List<?> values, Class<T> type, BiConsumer<Integer, T> writer)
    {
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value != null) {
                writer.accept(i, type.cast(value));
            }
            else {
                vector.setNull(i);
            }
        }
        vector.setValueCount(values.size());
    }

    private static void writeFieldVector(FieldVector vector, Type type, List<?> values)
    {
        if (!typedWriters.containsKey(type.getClass())) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        BiConsumer<FieldVectorContext, List<?>> writer = typedWriters.get(type.getClass());
        writer.accept(new FieldVectorContext(vector, type), values);
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        });
    }

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
//        testRoundTripType(type, true, insertNullEvery(5, readValues));
//        testRoundTripType(type, false, readValues);
//        testSimpleStructRoundTrip(type, readValues);
        testSimpleListRoundTrip(type, readValues);
    }

    public void testRoundTripType(Type type, boolean nullable, List<?> readValues)
            throws Exception
    {
        // For non-nullable tests, filter out null values
        List<?> filteredValues = nullable ? readValues : readValues.stream().filter(Objects::nonNull).collect(toList());
        assertRoundTrip(type, type, filteredValues, filteredValues, nullable);
    }

    private void testSimpleListRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type arrayType = arrayType(type);
        testRoundTripType(arrayType, false, values.stream().map(value -> ImmutableList.of(value, value, value)).collect(toList()));
//        testRoundTripType(arrayType, true, values.stream().map(value -> ImmutableList.of(value, value, value)).collect(toList()));

        // FIXME: do variable list size
//        testRoundTripType(arrayType, false, values.stream().map(value -> cycle(ImmutableList.of(value), random.nextLong(0, MAX_LIST_SIZE))).collect(toList()));
//        testRoundTripType(arrayType, true, insertNullEvery(9, values.stream().map(value -> cycle(ImmutableList.of(value), random.nextLong(0, MAX_LIST_SIZE))).collect(toList())));
    }

    private void testSimpleStructRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        testRoundTripType(rowType, false, values.stream().map(value -> List.of(value, value, value)).collect(toList()));
        testRoundTripType(rowType, true, insertNullEvery(7, values.stream().map(value -> List.of(value, value, value)).collect(toList())));
    }

    private void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues, boolean nullable)
            throws Exception
    {
        // write w/ JNI writer, read w/ LanceReader
        try (TempFile file = new TempFile()) {
            writeLanceColumnJNI(file.getFile(), writeType, writeValues, nullable);
            assertFileContentsTrino(readType, file, readValues);
        }
    }

    private static class FieldVectorContext
    {
        final FieldVector vector;
        final Type type;

        FieldVectorContext(FieldVector vector, Type type)
        {
            this.vector = vector;
            this.type = type;
        }
    }

    private static Type arrayType(Type elementType)
    {
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.typeParameter(elementType.getTypeSignature())));
    }

    private static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String fieldName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(fieldName)), fieldType.getTypeSignature())));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }

    public Field toArrowField(io.trino.lance.v2.metadata.Field field)
    {
        return switch (field.getLogicalType()) {
            case LogicalType.Int8Type _, LogicalType.Int16Type _, LogicalType.Int32Type _, LogicalType.Int64Type _, LogicalType.FloatType _, LogicalType.DoubleType _,
                 LogicalType.StringType _, LogicalType.BinaryType _ -> {
                FieldType fieldType = field.isNullable() ? FieldType.nullable(toArrowPrimitiveType(field)) : FieldType.notNullable(toArrowPrimitiveType(field));
                yield new Field(field.getName(), fieldType, ImmutableList.of());
            }
            case LogicalType.StructType _ -> {
                List<Field> childFields = field.getChildren().stream().map(this::toArrowField).collect(toImmutableList());
                FieldType fieldType = field.isNullable() ? FieldType.nullable(ArrowType.Struct.INSTANCE) : FieldType.notNullable(ArrowType.Struct.INSTANCE);
                yield new Field(field.getName(), fieldType, childFields);
            }
            case LogicalType.ListType _ -> {
                checkArgument(field.getChildren().size() == 1);
                FieldType fieldType = field.isNullable() ? FieldType.nullable(ArrowType.List.INSTANCE) : FieldType.notNullable(ArrowType.List.INSTANCE);
                yield new Field(field.getName(), fieldType, ImmutableList.of(toArrowField(field)));
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + field.getLogicalType());
        };
    }

    public ArrowType toArrowPrimitiveType(io.trino.lance.v2.metadata.Field field)
    {
        return switch (field.getLogicalType()) {
            case LogicalType.Int8Type _ -> ARROW_INT8_TYPE;
            case LogicalType.Int16Type _ -> ARROW_INT16_TYPE;
            case LogicalType.Int32Type _ -> ARROW_INT32_TYPE;
            case LogicalType.Int64Type _ -> ARROW_INT64_TYPE;
            case LogicalType.FloatType _ -> ARROW_REAL_TYPE;
            case LogicalType.DoubleType _ -> ARROW_DOUBLE_TYPE;
            case LogicalType.StringType _ -> ARROW_STRING_TYPE;
            case LogicalType.BinaryType _ -> ARROW_BINARY_TYPE;
            default -> throw new IllegalArgumentException("Unsupported type: " + field.getLogicalType());
        };
    }

    // populate arrow field vector
    public static class ArrowVectorGenerator
            extends TypeUtil.FieldVisitor<Map<Integer, FieldVector>>
    {
        private final Map<Integer, FieldVector> fieldVectorMap = new HashMap<>();

        public ArrowVectorGenerator() {}

        @Override
        public Map<Integer, FieldVector> primitive(io.trino.lance.v2.metadata.Field field)
        {
            return null;
        }

        @Override
        public Map<Integer, FieldVector> struct(io.trino.lance.v2.metadata.Field field, List<Map<Integer, FieldVector>> fieldResults)
        {
            return null;
        }

        @Override
        public Map<Integer, FieldVector> list(io.trino.lance.v2.metadata.Field field, Map<Integer, FieldVector> elementResult)
        {
            return null;
        }
    }

    public static VectorSchemaRoot populateVectorSchemaRoot(Schema schema, List<List<?>> data, BufferAllocator allocator)
    {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        int rowCount = data.size();
        List<Field> fields = schema.getFields();

        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            Field field = fields.get(fieldIndex);
            FieldVector vector = root.getVector(field.getName());

            // Extract column data for this field
            List<Object> columnData = new ArrayList<>();
            for (List<?> row : data) {
                columnData.add(row.get(fieldIndex));
            }

            populateFieldVector(vector, field, columnData);
        }

        root.setRowCount(rowCount);
        return root;
    }

    /**
     * Populates a single FieldVector with data.
     */
    private static void populateFieldVector(FieldVector vector, Field field, List<?> data)
    {
        ArrowType arrowType = field.getType();

        if (arrowType instanceof ArrowType.Int) {
            populateIntVector(vector, data, (ArrowType.Int) arrowType);
        }
        else if (arrowType instanceof ArrowType.FloatingPoint) {
            populateFloatingPointVector(vector, data, (ArrowType.FloatingPoint) arrowType);
        }
        else if (arrowType instanceof ArrowType.Utf8) {
            populateStringVector((VarCharVector) vector, data);
        }
        else if (arrowType instanceof ArrowType.Binary) {
            populateBinaryVector((VarBinaryVector) vector, data);
        }
        else if (arrowType instanceof ArrowType.Struct) {
            populateStructVector((StructVector) vector, field, data);
        }
        else if (arrowType instanceof ArrowType.List) {
            populateListVector((ListVector) vector, field, data);
        }
        else {
            throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
        }

        vector.setValueCount(data.size());
    }

    private static void populateIntVector(FieldVector vector, List<?> data, ArrowType.Int intType)
    {
        int bitWidth = intType.getBitWidth();
        boolean isSigned = intType.getIsSigned();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                Number num = (Number) value;
                switch (bitWidth) {
                    case 8:
                        ((TinyIntVector) vector).setSafe(i, num.byteValue());
                        break;
                    case 16:
                        ((SmallIntVector) vector).setSafe(i, num.shortValue());
                        break;
                    case 32:
                        ((IntVector) vector).setSafe(i, num.intValue());
                        break;
                    case 64:
                        ((BigIntVector) vector).setSafe(i, num.longValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported bit width: " + bitWidth);
                }
            }
        }
    }

    private static void populateFloatingPointVector(FieldVector vector, List<?> data, ArrowType.FloatingPoint floatType)
    {
        FloatingPointPrecision precision = floatType.getPrecision();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                Number num = (Number) value;
                switch (precision) {
                    case SINGLE:
                        ((Float4Vector) vector).setSafe(i, num.floatValue());
                        break;
                    case DOUBLE:
                        ((Float8Vector) vector).setSafe(i, num.doubleValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported precision: " + precision);
                }
            }
        }
    }

    private static void populateStringVector(VarCharVector vector, List<?> data)
    {
        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                String str = value.toString();
                vector.setSafe(i, str.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static void populateBinaryVector(VarBinaryVector vector, List<?> data)
    {
        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                byte[] bytes = (byte[]) value;
                vector.setSafe(i, bytes);
            }
        }
    }

    private static void populateStructVector(StructVector vector, Field field, List<?> data)
    {
        List<Field> childFields = field.getChildren();

        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i);
            if (value == null) {
                vector.setNull(i);
            }
            else {
                List<?> structData = (List<?>) value;
                vector.setIndexDefined(i);

                // Populate child vectors
                for (int childIndex = 0; childIndex < childFields.size(); childIndex++) {
                    Field childField = childFields.get(childIndex);
                    FieldVector childVector = vector.getChild(childField.getName());
                    Object childValue = structData.get(childIndex);

                    if (childValue == null) {
                        childVector.setNull(i);
                    }
                    else {
                        // For primitive types in struct, we need to handle them individually
                        populateStructChildVector(childVector, childField, i, childValue);
                    }
                }
            }
        }

        // Set value count for child vectors
        for (Field childField : childFields) {
            FieldVector childVector = vector.getChild(childField.getName());
            childVector.setValueCount(data.size());
        }
    }

    private static void populateStructChildVector(FieldVector childVector, Field childField, int index, Object value)
    {
        ArrowType arrowType = childField.getType();

        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            Number num = (Number) value;
            switch (intType.getBitWidth()) {
                case 8:
                    ((TinyIntVector) childVector).setSafe(index, num.byteValue());
                    break;
                case 16:
                    ((SmallIntVector) childVector).setSafe(index, num.shortValue());
                    break;
                case 32:
                    ((IntVector) childVector).setSafe(index, num.intValue());
                    break;
                case 64:
                    ((BigIntVector) childVector).setSafe(index, num.longValue());
                    break;
            }
        }
        else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
            Number num = (Number) value;
            switch (floatType.getPrecision()) {
                case SINGLE:
                    ((Float4Vector) childVector).setSafe(index, num.floatValue());
                    break;
                case DOUBLE:
                    ((Float8Vector) childVector).setSafe(index, num.doubleValue());
                    break;
            }
        }
        else if (arrowType instanceof ArrowType.Utf8) {
            String str = value.toString();
            ((VarCharVector) childVector).setSafe(index, str.getBytes(StandardCharsets.UTF_8));
        }
        else if (arrowType instanceof ArrowType.Binary) {
            byte[] bytes = (byte[]) value;
            ((VarBinaryVector) childVector).setSafe(index, bytes);
        }
//        else if (arrowType instanceof ArrowType.List) {
//            // Handle nested lists in struct
//            List<?> listData = (List<?>) value;
//            ListVector listVector = (ListVector) childVector;
//            populateListVector(listVector, childField, List.of(listData), index);
//        }
        else if (arrowType instanceof ArrowType.Struct) {
            // Handle nested structs
            List<?> structData = (List<?>) value;
            StructVector structVector = (StructVector) childVector;
            populateStructVector(structVector, childField, List.of(structData));
        }
        else {
            throw new UnsupportedOperationException("Unsupported ArrowType " + arrowType + " in struct field");
        }
    }

    public static void populateListVector(ListVector vector, Field field, List<?> data)
    {
        checkArgument(field.getChildren().size() == 1, "List field must have a single child");
        Field elementField = field.getChildren().getFirst();
        vector.setInitialCapacity(data.size());
        UnionListWriter writer = vector.getWriter();
        for (Object list : data) {
            writer.startList();
            for (Object value : (List<?>) list) {
                if (value == null) {
                    if (!elementField.isNullable()) {
                        throw new IllegalArgumentException("Cannot write null value to non-nullable array element field: " + elementField.getName());
                    }
                    writer.writeNull();
                } else {
                    switch (elementField.getType()) {
                        case ArrowType.Int intType -> {
                            switch (intType.getBitWidth()) {
                                case 8 -> writer.writeTinyInt((byte) value);
                                case 16 -> writer.writeSmallInt((short) value);
                                case 32 -> writer.writeInt((int) value);
                                case 64 -> writer.writeBigInt((long) value);
                            }
                        }
                        case ArrowType.FloatingPoint floatType -> {
                            switch (floatType.getPrecision()) {
                                case SINGLE -> writer.writeFloat4((float) value);
                                case DOUBLE -> writer.writeFloat8((double) value);
                            }
                        }
                        case ArrowType.Utf8 _ -> writer.writeVarChar((String) value);
                        case ArrowType.Binary _ -> writer.writeVarBinary((byte[]) value);
                        default -> throw new IllegalStateException("Unexpected value: " + elementField.getFieldType());
                    }
                }
            }
            writer.endList();
        }
        writer.setValueCount(data.size());
        vector.setValueCount(data.size());
    }
}
