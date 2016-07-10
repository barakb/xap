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
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

import java.util.Properties;

/**
 * Base class that delegates execution to data source.
 *
 * @author kimchy
 * @deprecated since 9.5
 */
@Deprecated
public abstract class AbstractManagedDataSourceDelegator implements ManagedDataSource {

    protected final ManagedDataSource dataSource;

    public AbstractManagedDataSourceDelegator(ManagedDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void init(Properties properties) throws DataSourceException {
        dataSource.init(properties);
    }

    public DataIterator initialLoad() throws DataSourceException {
        return dataSource.initialLoad();
    }

    public void shutdown() throws DataSourceException {
        dataSource.shutdown();
    }
}
