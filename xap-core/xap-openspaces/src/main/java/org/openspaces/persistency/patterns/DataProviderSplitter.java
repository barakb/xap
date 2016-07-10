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

package org.openspaces.persistency.patterns;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataProvider;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

/**
 * A data provider that redirects template based operations to the given data source that can handle
 * its type.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link SpaceDataSourceSplitter} instead.
 */
@Deprecated
public class DataProviderSplitter extends BulkDataPersisterSplitter implements DataProvider {

    public DataProviderSplitter(ManagedDataSourceEntriesProvider[] dataSources) {
        super(dataSources);
        for (ManagedDataSource dataSource : dataSources) {
            if (!(dataSource instanceof DataProvider)) {
                throw new IllegalArgumentException("data source [" + dataSource + "] must implement DataProvider");
            }
        }
    }

    public Object read(Object o) throws DataSourceException {
        return ((DataProvider) getDataSource(o.getClass().getName())).read(o);
    }

    public DataIterator iterator(Object o) throws DataSourceException {
        return ((DataProvider) getDataSource(o.getClass().getName())).iterator(o);
    }
}
