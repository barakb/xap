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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.range.InRange;
import com.j_spaces.sadapter.datasource.EntryAdapter;
import com.j_spaces.sadapter.datasource.IDataConverter;
import com.j_spaces.sadapter.datasource.SQLQueryBuilder;

import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * @author Dan Kilman
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class DataSourceIdsQueryImpl implements InternalDataSourceIdsQuery {

    private final ITypeDesc _typeDescriptor;
    private final Object[] _ids;
    private final SQLQueryBuilder _queryBuilder;
    private final Class<?> _dataClass;
    private final IDataConverter<IEntryPacket> _converter;
    private final IEntryHolder[] _templates;

    public DataSourceIdsQueryImpl(
            ITypeDesc typeDescriptor,
            Object[] ids,
            IEntryHolder[] templates,
            SQLQueryBuilder queryBuilder,
            Class<?> dataClass,
            IDataConverter<IEntryPacket> converter) {
        _typeDescriptor = typeDescriptor;
        _ids = ids;
        _templates = templates;
        _queryBuilder = queryBuilder;
        _dataClass = dataClass;
        _converter = converter;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return _typeDescriptor;
    }

    @Override
    public Object[] getIds() {
        return _ids;
    }

    @Override
    public DataSourceIdQuery getDataSourceIdQuery(int idIndex) {
        if (idIndex < 0 || idIndex >= _ids.length)
            throw new IllegalArgumentException("Index must match ids array. got=" +
                    idIndex + " while ids array length=" + _ids.length);

        IEntryHolder template = _templates[idIndex];
        EntryAdapter entryAdapter = new EntryAdapter(template,
                _typeDescriptor,
                _converter);
        return new DataSourceIdQueryImpl(_typeDescriptor,
                entryAdapter.toEntryPacket(),
                _queryBuilder,
                _dataClass,
                entryAdapter);
    }

    @Override
    public boolean supportsAsSQLQuery() {
        return _typeDescriptor.getIdPropertyName() != null;
    }

    @Override
    public DataSourceSQLQuery getAsSQLQuery() {
        if (!supportsAsSQLQuery())
            throw new UnsupportedOperationException();

        ICustomQuery query = new InRange(_typeDescriptor.getIdPropertyName(),
                // preserve ids order
                new LinkedHashSet<Object>(Arrays.asList(_ids)));
        SQLQuery<?> sqlQuery = query.toSQLQuery(_typeDescriptor);
        return new SQLQueryToDataSourceSQLQueryAdapter(sqlQuery);
    }

}
