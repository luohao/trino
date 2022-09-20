package io.trino.plugin.tidb.util;

import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.tikv.common.types.AbstractDateTimeType;
import org.tikv.common.types.DataType;
import org.tikv.common.types.DateType;
import org.tikv.common.types.EnumType;
import org.tikv.common.types.JsonType;
import org.tikv.common.types.SetType;
import org.tikv.common.types.StringType;

import static io.trino.spi.type.DateType.DATE;

public class TypeMapping
{
    private static boolean isStringType(DataType type)
    {
        return type instanceof EnumType
                || type instanceof JsonType
                || type instanceof SetType
                || type instanceof StringType;
    }

    public static Type toTrinoType(DataType type)
    {
        if (type instanceof DateType) {
            return DATE;
        }
        if (type instanceof AbstractDateTimeType) {
            return TimestampType;
        }

    }
}
