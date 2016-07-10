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

package org.openspaces.test.common.mock;

import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

public class MockDataSourceQuery implements DataSourceQuery {

    private final SpaceDocument query;
    private final SpaceTypeDescriptor typeDescriptor;
    private final int maxResults;
    private final DataSourceSQLQuery sqlQuery;

    public MockDataSourceQuery(SpaceTypeDescriptor typeDescriptor, SpaceDocument query, int maxResults) {
        this.typeDescriptor = typeDescriptor;
        this.maxResults = maxResults;
        this.query = query;
        this.sqlQuery = null;
    }

    public MockDataSourceQuery(SpaceTypeDescriptor typeDescriptor, DataSourceSQLQuery sqlQuery, int maxResults) {
        this.typeDescriptor = typeDescriptor;
        this.maxResults = maxResults;
        this.query = null;
        this.sqlQuery = sqlQuery;
    }

    @Override
    public DataSourceSQLQuery getAsSQLQuery() {
        return sqlQuery;
    }

    @Override
    public Object getTemplateAsObject() {
        return null;
    }

    @Override
    public SpaceDocument getTemplateAsDocument() {
        return query;
    }

    @Override
    public boolean supportsAsSQLQuery() {
        return sqlQuery != null;
    }

    @Override
    public boolean supportsTemplateAsObject() {
        return false;
    }

    @Override
    public boolean supportsTemplateAsDocument() {
        return query != null;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
    }

    @Override
    public int getBatchSize() {
        return maxResults;
    }
}
