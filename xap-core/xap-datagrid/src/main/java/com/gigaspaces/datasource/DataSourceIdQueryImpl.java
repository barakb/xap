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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.EntryAdapter;
import com.j_spaces.sadapter.datasource.SQLQueryBuilder;

/**
 * @author Idan Moyal
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class DataSourceIdQueryImpl
        implements DataSourceIdQuery, DataSourceQuery {
    private final ITypeDesc _typeDescriptor;
    private final SQLQueryBuilder _queryBuilder;
    private final EntryAdapter _entryAdapter;
    private final IEntryPacket _entryPacket;

    public DataSourceIdQueryImpl(ITypeDesc typeDescriptor, IEntryPacket entryPacket,
                                 SQLQueryBuilder queryBuilder, Class<?> dataClass,
                                 EntryAdapter entryAdapter) {
        _typeDescriptor = typeDescriptor;
        _entryPacket = entryPacket;
        _queryBuilder = queryBuilder;
        _entryAdapter = entryAdapter;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return _typeDescriptor;
    }

    @Override
    public Object getId() {
        return _entryPacket.getID();
    }

    @Override
    public int getVersion() {
        return _entryPacket.getVersion();
    }

    @Override
    public String toString() {
        return "DataSourceIdQuery [id=" + getId() + ", typeName="
                + _typeDescriptor.getTypeName() + ", version="
                + getVersion() + "]";
    }

    @Override
    public DataSourceSQLQuery getAsSQLQuery() {
        if (!supportsAsSQLQuery())
            throw new UnsupportedOperationException();
        final SQLQuery<?> sqlQuery = _queryBuilder.build(_entryPacket, (ITypeDesc) _typeDescriptor);
        return new SQLQueryToDataSourceSQLQueryAdapter(sqlQuery);
    }

    @Override
    public Object getTemplateAsObject() {
        if (!supportsTemplateAsObject())
            throw new UnsupportedOperationException();
        return _entryAdapter.toObject();
    }

    @Override
    public SpaceDocument getTemplateAsDocument() {
        if (!supportsTemplateAsDocument())
            throw new UnsupportedOperationException();
        return _entryAdapter.toDocument();
    }

    @Override
    public boolean supportsAsSQLQuery() {
        return true;
    }

    @Override
    public boolean supportsTemplateAsObject() {
        return _typeDescriptor.getIdentifierPropertyId() != -1;
    }

    @Override
    public boolean supportsTemplateAsDocument() {
        return supportsTemplateAsObject();
    }

    @Override
    public int getBatchSize() {
        return 1;
    }

}
