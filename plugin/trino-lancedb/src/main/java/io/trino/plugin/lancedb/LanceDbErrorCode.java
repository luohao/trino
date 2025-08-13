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
package io.trino.plugin.lancedb;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum LanceDbErrorCode
        implements ErrorCodeSupplier
{
    // FIXME: split into LanceErrorCode and LanceDbErrorCode
    LANCEDB_INVALID_METADATA(1, EXTERNAL),
    LANCEDB_BAD_DATA(2, EXTERNAL),
    LANCEDB_SPLIT_ERROR(3, EXTERNAL),
    LANCEDB_INVALID_VERSION_NUMBER(11, USER_ERROR)
    /**/;
    private final ErrorCode errorCode;

    LanceDbErrorCode(int code, ErrorType errorType)
    {
        errorCode = new ErrorCode(code + 0x0524_0000, name(), errorType, errorType == USER_ERROR);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
