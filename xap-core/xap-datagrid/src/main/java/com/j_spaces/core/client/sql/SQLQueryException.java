/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.core.client.sql;

import com.j_spaces.core.client.SQLQuery;

/**
 * SQLQueryException is thrown when space operation with {@link SQLQuery} fails.<br> For example
 * illegal SQL syntax, semantic error.
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SQLQueryException extends RuntimeException {
    private static final long serialVersionUID = 5077802730807491070L;

    public SQLQueryException() {
    }

    public SQLQueryException(String message) {
        super(message);
    }

    public SQLQueryException(Throwable cause) {
        super(cause);
    }

    public SQLQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
