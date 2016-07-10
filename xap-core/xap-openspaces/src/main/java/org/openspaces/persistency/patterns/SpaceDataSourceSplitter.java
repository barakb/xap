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
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import org.openspaces.persistency.support.ConcurrentMultiDataIterator;
import org.openspaces.persistency.support.SerialMultiDataIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A space data source that redirects the query operations to the given data source that can handle
 * the given type.
 *
 * @author eitany
 * @since 9.5
 */
public class SpaceDataSourceSplitter extends SpaceDataSource {

    private final ManagedEntriesSpaceDataSource[] dataSources;

    private final Map<String, SpaceDataSource> entriesToDataSource = new HashMap<String, SpaceDataSource>();

    private int initalLoadThreadPoolSize = 10;

    public SpaceDataSourceSplitter(ManagedEntriesSpaceDataSource[] dataSources) {
        this.dataSources = dataSources;
        for (ManagedEntriesSpaceDataSource dataSource : dataSources) {
            for (String entry : dataSource.getManagedEntries()) {
                entriesToDataSource.put(entry, dataSource);
            }
        }
    }

    public void setInitalLoadThreadPoolSize(int initalLoadThreadPoolSize) {
        this.initalLoadThreadPoolSize = initalLoadThreadPoolSize;
    }


    protected SpaceDataSource getDataSource(String entry) {
        return entriesToDataSource.get(entry);
    }

    /**
     * Iterates through all the given data sources and assembles their respective {@link
     * com.gigaspaces.datasource.DataIterator}s from {@link com.gigaspaces.datasource.SpaceDataSource#initialDataLoad()}.
     * Constructs a {@link org.openspaces.persistency.support.ConcurrentMultiDataIterator} on top of
     * them.
     */
    @SuppressWarnings("unchecked")
    @Override
    public DataIterator<Object> initialDataLoad() {
        ArrayList<DataIterator> iterators = new ArrayList<DataIterator>(dataSources.length);
        for (SpaceDataSource dataSource : dataSources) {
            DataIterator<Object> iterator = dataSource.initialDataLoad();
            if (iterator != null)
                iterators.add(iterator);
        }
        return new ConcurrentMultiDataIterator(iterators.toArray(new DataIterator[iterators.size()]), initalLoadThreadPoolSize);
    }

    /**
     * Iterates through all the given data sources and assembles their respective {@link
     * com.gigaspaces.datasource.DataIterator}s from {@link com.gigaspaces.datasource.SpaceDataSource#initialMetadataLoad()}.
     * Constructs a {@link org.openspaces.persistency.support.SerialMultiDataIterator} on top of
     * them.
     */
    @SuppressWarnings("unchecked")
    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        ArrayList<DataIterator<SpaceTypeDescriptor>> iterators = new ArrayList<DataIterator<SpaceTypeDescriptor>>(dataSources.length);
        for (SpaceDataSource dataSource : dataSources) {
            DataIterator<SpaceTypeDescriptor> iterator = dataSource.initialMetadataLoad();
            if (iterator != null)
                iterators.add(iterator);
        }
        return new SerialMultiDataIterator(iterators.toArray(new DataIterator[iterators.size()]));
    }

    /**
     * Delegates the query to the corresponding data source
     */
    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        SpaceDataSource dataSource = getDataSource(query.getTypeDescriptor().getTypeName());
        return dataSource == null ? null : dataSource.getDataIterator(query);
    }

    /**
     * Delegates the query to the corresponding data source
     */
    @Override
    public Object getById(DataSourceIdQuery idQuery) {
        SpaceDataSource dataSource = getDataSource(idQuery.getTypeDescriptor().getTypeName());
        return dataSource == null ? null : dataSource.getById(idQuery);
    }

    /**
     * Delegates the query to the corresponding data source
     */
    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        SpaceDataSource dataSource = getDataSource(idsQuery.getTypeDescriptor().getTypeName());
        return dataSource == null ? null : dataSource.getDataIteratorByIds(idsQuery);
    }
}
