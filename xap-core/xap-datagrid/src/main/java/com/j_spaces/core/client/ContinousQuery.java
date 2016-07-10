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


package com.j_spaces.core.client;

import com.gigaspaces.query.QueryResultType;
import com.j_spaces.core.client.view.View;

/**
 * @see View
 * @see SQLQuery
 * @deprecated Since 8.0 - This class has been deprecated and should not be used, use {@link View}
 * or {@link SQLQuery} instead.
 */
@Deprecated
public class ContinousQuery<T> extends SQLQuery<T> {
    private static final long serialVersionUID = -5931704877403562166L;

    /**
     * Empty constructor.
     */
    public ContinousQuery() {
    }

    /**
     * Creates a ContinousQuery using the specified type and expression.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public ContinousQuery(String typeName, String sqlExpression) {
        super(typeName, sqlExpression);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression and result type.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0
     */
    public ContinousQuery(String typeName, String sqlExpression, QueryResultType queryResultType) {
        super(typeName, sqlExpression, queryResultType);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression and parameters.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     * @since 7.0
     */
    public ContinousQuery(String typeName, String sqlExpression, Object... parameters) {
        super(typeName, sqlExpression, parameters);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression, result type and parameters.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0
     */
    public ContinousQuery(String typeName, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        super(typeName, sqlExpression, queryResultType, parameters);
    }

    /**
     * Creates a ContinousQuery using the specified type and expression.
     *
     * @param type          Entry class  to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public ContinousQuery(Class<T> type, String sqlExpression) {
        super(type, sqlExpression);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression and result type.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0
     */
    public ContinousQuery(Class<T> type, String sqlExpression, QueryResultType queryResultType) {
        super(type, sqlExpression, queryResultType);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression and parameters.
     *
     * @param type          Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     * @since 7.0
     */
    public ContinousQuery(Class<T> type, String sqlExpression, Object... parameters) {
        super(type, sqlExpression, parameters);
    }

    /**
     * Creates a ContinousQuery using the specified type, expression, result type and parameters.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0
     */
    public ContinousQuery(Class<T> type, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        super(type, sqlExpression, queryResultType, parameters);
    }
}
