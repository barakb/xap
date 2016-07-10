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
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.EntryAdapter;
import com.j_spaces.sadapter.datasource.SQLQueryBuilder;

/**
 * @author Idan Moyal
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class DataSourceQueryImpl
        implements DataSourceQuery {
    private final ITemplateHolder _template;
    private final Class<?> _dataClass;
    private final SQLQueryBuilder _queryBuilder;
    private final EntryAdapter _entryAdapter;
    private final int _batchSize;
    private final ITypeDesc _typeDescriptor;

    private SQLQuery<?> _sqlQuery;

    public DataSourceQueryImpl(ITemplateHolder template,
                               ITypeDesc typeDescriptor, Class<?> dataClass,
                               SQLQueryBuilder queryBuilder, EntryAdapter entryAdapter,
                               int batchSize) {
        _template = template;
        _typeDescriptor = typeDescriptor;
        _dataClass = dataClass;
        _queryBuilder = queryBuilder;
        _entryAdapter = entryAdapter;
        _batchSize = batchSize;
    }

    @Override
    public DataSourceSQLQuery getAsSQLQuery() {
        if (!supportsAsSQLQuery())
            throw new UnsupportedOperationException();
        if (_sqlQuery == null)
            _sqlQuery = _queryBuilder.build(_template, _typeDescriptor.getTypeName(), _typeDescriptor);
        return new SQLQueryToDataSourceSQLQueryAdapter(_sqlQuery);
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
        // On inheritance, entry adapter is not correlated to the typeDescriptor member
        // but still document creation for the derived type is possible and therefore the type name
        // is provided for the conversion.
        return _entryAdapter.toDocument(_typeDescriptor.getTypeName());
    }

    @Override
    public boolean supportsAsSQLQuery() {
        return true;
    }

    @Override
    public boolean supportsTemplateAsObject() {
        if (IEntryPacket.class.isAssignableFrom(_dataClass))
            return true;

        if (_template.getCustomQuery() != null)
            return false;

        if (IGSEntry.class.isAssignableFrom(_dataClass))
            return true;

        if (_template.hasExtendedMatchCodes())
            return false;

        return _typeDescriptor.getTypeName().equals(_template.getClassName())
                && _typeDescriptor.isConcreteType();
    }

    @Override
    public boolean supportsTemplateAsDocument() {
        if (IEntryPacket.class.isAssignableFrom(_dataClass))
            return true;

        if (_template.getCustomQuery() != null)
            return false;

        if (IGSEntry.class.isAssignableFrom(_dataClass))
            return true;

        if (_template.hasExtendedMatchCodes()) {
            return false;
        }
        return _template.getClassName() != null;
    }

    @Override
    public int getBatchSize() {
        return _batchSize;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return _typeDescriptor;
    }

    @Override
    public String toString() {
        return "DataSourceQuery [typeName = "
                + _typeDescriptor.getTypeName()
                + ", supportsTemplateAsObject=" + supportsTemplateAsObject()
                + ", supportsTemplateAsDocument="
                + supportsTemplateAsDocument()
                + ", supportsTemplateAsSqlQuery=" + supportsAsSQLQuery()
                + ", batchSize=" + _batchSize + "]";
    }


}
