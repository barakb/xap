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

package org.openspaces.persistency.cassandra.archive;

import org.openspaces.core.GigaSpace;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;

public class CassandraArchiveOperationHandlerConfigurer {

    CassandraArchiveOperationHandler handler;
    private boolean initialized;

    public CassandraArchiveOperationHandlerConfigurer() {
        handler = new CassandraArchiveOperationHandler();
    }

    /**
     * @see CassandraArchiveOperationHandler#setKeyspace(String)
     */
    public CassandraArchiveOperationHandlerConfigurer keyspace(String keyspace) {
        handler.setKeyspace(keyspace);
        return this;
    }

    /**
     * @see CassandraArchiveOperationHandler#setHosts(String)
     */
    public CassandraArchiveOperationHandlerConfigurer hosts(String hosts) {
        handler.setHosts(hosts);
        return this;
    }

    /**
     * @see CassandraArchiveOperationHandler#setPort(Integer)
     */
    public CassandraArchiveOperationHandlerConfigurer port(int port) {
        handler.setPort(port);
        return this;
    }

    /**
     * @see CassandraArchiveOperationHandler#setWriteConsistency(org.openspaces.persistency.cassandra.CassandraConsistencyLevel)
     */
    public CassandraArchiveOperationHandlerConfigurer writeConsistency(CassandraConsistencyLevel writeConsistency) {
        handler.setWriteConsistency(writeConsistency);
        return this;
    }

    /**
     * @see CassandraArchiveOperationHandler#setGigaSpace(GigaSpace)
     */
    public CassandraArchiveOperationHandlerConfigurer gigaSpace(GigaSpace gigaSpace) {
        handler.setGigaSpace(gigaSpace);
        return this;
    }

    public CassandraArchiveOperationHandler create() {
        if (!initialized) {
            handler.afterPropertiesSet();
            initialized = true;
        }
        return handler;
    }
}
