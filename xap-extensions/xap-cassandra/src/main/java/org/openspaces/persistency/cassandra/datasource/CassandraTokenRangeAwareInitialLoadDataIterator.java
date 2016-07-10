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

package org.openspaces.persistency.cassandra.datasource;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;

import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.pool.ConnectionResource;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraTokenRangeAwareInitialLoadDataIterator implements DataIterator<Object> {

    private final SpaceDocumentColumnFamilyMapper mapper;
    private final Map<String, ColumnFamilyMetadata> metadataMap;
    private final Iterator<Map.Entry<String, ColumnFamilyMetadata>> metadata;
    private final ConnectionResource connectionResource;
    private final Map<String, String> initialLoadQueries;
    private final int batchLimit;

    private final CassandraConsistencyLevel readConsistencyLevel;
    private CassandraTokenRangeAwareDataIterator currentIterator;

    public CassandraTokenRangeAwareInitialLoadDataIterator(
            SpaceDocumentColumnFamilyMapper mapper,
            Map<String, ColumnFamilyMetadata> metadataMap,
            ConnectionResource connectionResource,
            Map<String, String> queries,
            int batchLimit,
            CassandraConsistencyLevel readConsistencyLevel) {
        this.mapper = mapper;
        this.batchLimit = batchLimit;
        this.connectionResource = connectionResource;
        this.initialLoadQueries = queries;
        this.readConsistencyLevel = readConsistencyLevel;
        this.metadataMap = metadataMap;
        this.metadata = metadataMap.entrySet().iterator();
        this.currentIterator = nextDataIterator();
    }

    @Override
    public boolean hasNext() {
        while (currentIterator != null && !currentIterator.hasNext()) {
            currentIterator = nextDataIterator();
        }

        return currentIterator != null;
    }

    @Override
    public SpaceDocument next() {
        return currentIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported for this iterator");
    }

    @Override
    public void close() {
        if (currentIterator != null) {
            currentIterator.closeSelfResources();
        }
        connectionResource.release();
    }

    private CassandraTokenRangeAwareDataIterator nextDataIterator() {
        if (metadata.hasNext()) {
            Map.Entry<String, ColumnFamilyMetadata> metadata = this.metadata.next();

            CQLQueryContext queryContext = null; // this default value will result in a 'select all' query
            if (initialLoadQueries != null && initialLoadQueries.containsKey(metadata.getKey())) {
                // the initial load query is hard-coded by the user as-is, shouldn't need parameters
                queryContext = new CQLQueryContext(null, initialLoadQueries.get(metadata.getKey()), new Object[]{});
            }

            return new CassandraTokenRangeAwareDataIterator(mapper,
                    metadata.getValue(),
                    connectionResource,
                    queryContext,
                    Integer.MAX_VALUE,
                    batchLimit,
                    readConsistencyLevel);
        } else {
            return null;
        }
    }
}
