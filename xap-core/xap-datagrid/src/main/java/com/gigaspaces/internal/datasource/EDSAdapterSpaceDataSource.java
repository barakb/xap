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

package com.gigaspaces.internal.datasource;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.datasource.SpaceDataSourceException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.DataStorage;

import java.util.Properties;

/**
 * @author idan
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class EDSAdapterSpaceDataSource extends SpaceDataSource {

    private final DataStorage<Object> _storage;
    private final boolean _supportsInheritance;

    public EDSAdapterSpaceDataSource(DataStorage<Object> dataStorage, boolean supportsInheritance) {
        this._storage = dataStorage;
        _supportsInheritance = supportsInheritance;
    }

    @Override
    public DataIterator<Object> initialDataLoad() {
        if (_storage.isManagedDataSource()) {
            try {
                return _storage.initialLoad();
            } catch (DataSourceException e) {
                throw new SpaceDataSourceException(e);
            }
        }
        return null;
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return null;
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        try {
            if (_storage.isSQLDataProvider()) {
                final DataSourceSQLQuery sqlQueryInfo = query.getAsSQLQuery();
                final SQLQuery<Object> sqlQuery = new SQLQuery<Object>(query.getTypeDescriptor()
                        .getTypeName(),
                        sqlQueryInfo.getQuery(),
                        sqlQueryInfo.getQueryParameters());
                return _storage.iterator(sqlQuery);
            } else if (_storage.isDataProvider() && query.supportsTemplateAsObject()) {
                return _storage.iterator(query.getTemplateAsObject());
            }
        } catch (DataSourceException e) {
            throw new SpaceDataSourceException(e);
        }
        return null;
    }

    @Override
    public boolean supportsInheritance() {
        return _supportsInheritance;
    }

    public void initialize(Properties dataProperties) throws DataSourceException {
        if (_storage.isManagedDataSource())
            _storage.init(dataProperties);
    }

    public DataStorage<Object> getDataStorage() {
        return _storage;
    }

}