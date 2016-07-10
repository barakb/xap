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

import org.openspaces.persistency.support.ConcurrentMultiDataIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A base class for a splitter data source. Accepts a list of {@link
 * org.openspaces.persistency.patterns.ManagedDataSourceEntriesProvider}s and based on their
 * respective managed entries will split operations to the ones that control a given entry.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link SpaceDataSourceSplitter} or {@link
 * SpaceSynchronizationEndpointSplitter} instead.
 */
@Deprecated
public class AbstractManagedDataSourceSplitter implements ManagedDataSource {

    protected final ManagedDataSourceEntriesProvider[] dataSources;

    private int initalLoadThreadPoolSize = 10;

    private Map<String, ManagedDataSource> entriesToDataSource = new HashMap<String, ManagedDataSource>();

    public AbstractManagedDataSourceSplitter(ManagedDataSourceEntriesProvider[] dataSources) {
        this.dataSources = dataSources;
        for (ManagedDataSourceEntriesProvider dataSource : dataSources) {
            for (String entry : dataSource.getManagedEntries()) {
                entriesToDataSource.put(entry, dataSource);
            }
        }
    }

    public void setInitalLoadThreadPoolSize(int initalLoadThreadPoolSize) {
        this.initalLoadThreadPoolSize = initalLoadThreadPoolSize;
    }

    /**
     * Iterates through all the given data sources and calls {@link com.gigaspaces.datasource.ManagedDataSource#init(java.util.Properties)}
     * on them.
     */
    public void init(Properties properties) throws DataSourceException {
        for (ManagedDataSource dataSource : dataSources) {
            dataSource.init(properties);
        }
    }

    /**
     * Iterates through all the given data sources and assembles their respective {@link
     * com.gigaspaces.datasource.DataIterator}s from {@link com.gigaspaces.datasource.ManagedDataSource#initialLoad()}.
     * Constructs a {@link org.openspaces.persistency.support.ConcurrentMultiDataIterator} on top of
     * them.
     */
    public DataIterator initialLoad() throws DataSourceException {
        ArrayList<DataIterator> iterators = new ArrayList<DataIterator>();
        for (ManagedDataSource dataSource : dataSources) {
            iterators.add(dataSource.initialLoad());
        }
        return new ConcurrentMultiDataIterator(iterators.toArray(new DataIterator[iterators.size()]), initalLoadThreadPoolSize);
    }

    public void shutdown() throws DataSourceException {
        for (ManagedDataSource dataSource : dataSources) {
            dataSource.shutdown();
        }
    }

    protected ManagedDataSource getDataSource(String entry) {
        return entriesToDataSource.get(entry);
    }
}
