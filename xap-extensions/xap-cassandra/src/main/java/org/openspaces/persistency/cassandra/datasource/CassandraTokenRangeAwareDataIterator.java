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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.CassandraSpaceDataSource;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.pool.ConnectionResource;

/**
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraTokenRangeAwareDataIterator implements DataIterator<Object> {

    private static final Log logger = LogFactory.getLog(CassandraSpaceDataSource.class);

    private final ConnectionResource connectionResource;
    private final SpaceDocumentColumnFamilyMapper mapper;
    private final ColumnFamilyMetadata columnFamilyMetadata;
    private final int maxResults;
    private final int batchLimit;
    private final CQLQueryContext queryContext;
    private final CassandraConsistencyLevel readConsistencyLevel;

    private boolean closed = false;
    private CassandraTokenRangeJDBCDataIterator currentIterator;
    private Object currentLastToken = null;
    private int currentResultCount = 0;


    public CassandraTokenRangeAwareDataIterator(
            SpaceDocumentColumnFamilyMapper mapper,
            ColumnFamilyMetadata columnFamilyMetadata,
            ConnectionResource connectionResource,
            CQLQueryContext queryContext,
            int maxResults,
            int batchLimit,
            CassandraConsistencyLevel readConsistencyLevel) {
        if (logger.isTraceEnabled()) {
            logger.trace("Creating data iterator for query: " + queryContext + " for type: " + columnFamilyMetadata.getTypeName() +
                    ", batchLimit=" + batchLimit);
        }

        this.mapper = mapper;
        this.columnFamilyMetadata = columnFamilyMetadata;
        this.connectionResource = connectionResource;
        this.queryContext = queryContext;
        this.maxResults = maxResults;
        this.batchLimit = batchLimit;
        this.readConsistencyLevel = readConsistencyLevel;
        this.currentIterator = nextDataIterator();
    }

    @Override
    public boolean hasNext() {
        while (currentIterator != null && !currentIterator.hasNext()) {
            currentIterator.closeSelfResources();
            currentIterator = nextDataIterator();
        }

        return currentIterator != null;
    }

    @Override
    public SpaceDocument next() {
        currentResultCount++;
        return currentIterator.next();
    }

    private CassandraTokenRangeJDBCDataIterator nextDataIterator() {
        if (calculateRemainingResults() <= 0) {
            return null;
        }

        // indication this is the first time nextDataIterator() is called
        // so no last token exists yet
        if (currentIterator == null) {
            CassandraTokenRangeJDBCDataIterator result = createIterator();

            // no need to continue with other iterators if this query returned no results
            // this will cause the next call to calculateRemainingResults() to return 0
            // thus ending our iterations
            if (result.getLastToken() == null) {
                currentResultCount = maxResults;
            }

            return result;
        } else {
            currentLastToken = currentIterator.getLastToken();
            if (currentLastToken == null ||
                    currentIterator.getCurrentTotalCount() < currentIterator.getLimit()) {
                // finish iteration condition
                return null;
            } else {
                return createIterator();
            }
        }
    }

    private CassandraTokenRangeJDBCDataIterator createIterator() {
        return new CassandraTokenRangeJDBCDataIterator(mapper,
                columnFamilyMetadata,
                connectionResource,
                queryContext,
                currentLastToken /* last token is used */,
                calculateRemainingResults(),
                readConsistencyLevel);
    }

    private int calculateRemainingResults() {
        int maxRemaining = maxResults == Integer.MAX_VALUE ? Integer.MAX_VALUE :
                maxResults - currentResultCount;
        return maxRemaining >= batchLimit ? batchLimit : maxRemaining;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }

    public void closeSelfResources() {
        if (currentIterator != null) {
            currentIterator.closeSelfResources();
            currentIterator = null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        try {
            closeSelfResources();
            connectionResource.release();
        } finally {
            closed = true;
        }
    }
}
