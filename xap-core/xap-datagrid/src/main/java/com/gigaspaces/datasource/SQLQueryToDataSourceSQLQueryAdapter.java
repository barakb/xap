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

import com.j_spaces.core.client.SQLQuery;

/**
 * @author eitany
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class SQLQueryToDataSourceSQLQueryAdapter implements
        DataSourceSQLQuery {

    private final SQLQuery<?> _sqlQuery;

    public SQLQueryToDataSourceSQLQueryAdapter(SQLQuery<?> sqlQuery) {
        this._sqlQuery = sqlQuery;
    }

    @Override
    public String getQuery() {
        return _sqlQuery.getQuery();
    }

    @Override
    public String getFromQuery() {
        return _sqlQuery.getFromQuery();
    }

    @Override
    public String getSelectAllQuery() {
        return _sqlQuery.getSelectAllQuery();
    }

    @Override
    public String getSelectCountQuery() {
        return _sqlQuery.getSelectCountQuery();
    }

    @Override
    public Object[] getQueryParameters() {
        return _sqlQuery.getParameters();
    }

    @Override
    public int hashCode() {
        return _sqlQuery.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof SQLQueryToDataSourceSQLQueryAdapter))
            return false;

        SQLQueryToDataSourceSQLQueryAdapter adapter = (SQLQueryToDataSourceSQLQueryAdapter) obj;
        return _sqlQuery.equals(adapter._sqlQuery);
    }

    @Override
    public String toString() {
        return _sqlQuery.toString();
    }

}
