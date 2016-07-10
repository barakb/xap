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

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A bulk data persister that implements the execute bulk operation. Reshuffles the given list of
 * bulk items by grouping them based on the types and then calls the executeBulk for each type
 * respective persister.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link SpaceSynchronizationEndpointSplitter} instead.
 */
@Deprecated
public class BulkDataPersisterSplitter extends AbstractManagedDataSourceSplitter implements BulkDataPersister {

    public BulkDataPersisterSplitter(ManagedDataSourceEntriesProvider[] dataSources) {
        super(dataSources);
        for (ManagedDataSource dataSource : dataSources) {
            if (!(dataSource instanceof BulkDataPersister)) {
                throw new IllegalArgumentException("data source [" + dataSource + "] must implement BulkDataPersister");
            }
        }
    }

    public void executeBulk(List<BulkItem> bulkItems) throws DataSourceException {
        Map<String, List<BulkItem>> bulkItemsByEntries = new HashMap<String, List<BulkItem>>();
        for (BulkItem bulkItem : bulkItems) {
            List<BulkItem> items = bulkItemsByEntries.get(bulkItem.getItem().getClass().getName());
            if (items == null) {
                items = new ArrayList<BulkItem>();
                bulkItemsByEntries.put(bulkItem.getItem().getClass().getName(), items);
            }
            items.add(bulkItem);
        }

        for (Map.Entry<String, List<BulkItem>> entry : bulkItemsByEntries.entrySet()) {
            BulkDataPersister bulkDataPersister = (BulkDataPersister) getDataSource(entry.getKey());
            if (bulkDataPersister != null) {
                bulkDataPersister.executeBulk(entry.getValue());
            }
        }
    }
}
