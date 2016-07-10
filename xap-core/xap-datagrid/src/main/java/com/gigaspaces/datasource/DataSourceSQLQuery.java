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

package com.gigaspaces.datasource;

/**
 * Represents a parameterized SQL query of a {@link DataSourceQuery} instance.
 *
 * @author eitany
 * @author Idan Moyal
 * @since 9.1.1
 */
public interface DataSourceSQLQuery {
    /**
     * @return A parameterized SQL query string representation of the query ("WHERE" part only).
     */
    String getQuery();

    /**
     * @return A parameterized SQL query string representation of the query in the form of: <p> FROM
     * <tt>table name</tt> WHERE <tt>query expression</tt> </p>
     */
    String getFromQuery();

    /**
     * @return A parameterized SQL query string representation of the query in the form of: <p>
     * SELECT * FROM <tt>table name</tt> WHERE <tt>query expression</tt> </p>
     */
    String getSelectAllQuery();

    /**
     * @return A parameterized SQL query string representation of the query in the form of: <p>
     * SELECT count(*) FROM <tt>table name</tt> WHERE <tt>query expression</tt> </p>
     */
    String getSelectCountQuery();

    /**
     * @return The parameters as object array to be used with the query's parameterized string.
     */
    Object[] getQueryParameters();
}
