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


package com.j_spaces.core.client.view;

import com.gigaspaces.query.QueryResultType;
import com.j_spaces.core.client.ContinousQuery;

/**
 * Extends the {@link ContinousQuery} interface to allow creation of local view.<br> Code Example:
 * <pre><code>
 *    {@link java.util.Properties Properties} prop = new Properties();
 * 	View[] views = new View[]{ new View( MyClass, "myField>10")};
 * 	prop.put( {@link com.j_spaces.core.client.SpaceURL#VIEWS SpaceURL.VIEWS}, views);
 *    {@link com.j_spaces.core.client.SpaceFinder#find(String, java.util.Properties)
 * SpaceFinder.find(
 * spaceUrl, prop)};
 * </code></pre>
 *
 * @author Guy Korland
 * @version 1.0
 * @see com.j_spaces.core.client.SpaceFinder
 * @since 5.2
 * @deprecated Since 8.0.5 - Use SQLQuery<T> Instead.
 */
@Deprecated
public class View<T> extends ContinousQuery<T> {
    private static final long serialVersionUID = 1914367097057713706L;

    /**
     * Empty constructor.
     */
    public View() {
    }

    /**
     * Creates a View using the specified type and expression.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public View(String typeName, String sqlExpression) {
        super(typeName, sqlExpression);
    }

    /**
     * Creates a View using the specified type, expression and result type.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0.1
     */
    public View(String typeName, String sqlExpression, QueryResultType queryResultType) {
        super(typeName, sqlExpression, queryResultType);
    }

    /**
     * Creates a View using the specified type, expression and parameters.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     * @since 7.0
     */
    public View(String typeName, String sqlExpression, Object... parameters) {
        super(typeName, sqlExpression, parameters);
    }

    /**
     * Creates a View using the specified type, expression, result type and parameters.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0.1
     */
    public View(String typeName, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        super(typeName, sqlExpression, queryResultType, parameters);
    }

    /**
     * Creates a View using the specified type and expression.
     *
     * @param type          Entry class  to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public View(Class<T> type, String sqlExpression) {
        super(type, sqlExpression);
    }

    /**
     * Creates a View using the specified type, expression and result type.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0.1
     */
    public View(Class<T> type, String sqlExpression, QueryResultType queryResultType) {
        super(type, sqlExpression, queryResultType);
    }

    /**
     * Creates a View using the specified type, expression and parameters.
     *
     * @param type          Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     * @since 7.0
     */
    public View(Class<T> type, String sqlExpression, Object... parameters) {
        super(type, sqlExpression, parameters);
    }

    /**
     * Creates a View using the specified type, expression, result type and parameters.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0.1
     */
    public View(Class<T> type, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        super(type, sqlExpression, queryResultType, parameters);
    }
}
