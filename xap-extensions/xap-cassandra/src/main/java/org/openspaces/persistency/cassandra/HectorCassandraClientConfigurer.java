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

package org.openspaces.persistency.cassandra;

import org.springframework.util.StringUtils;

import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

/**
 * A configurer for creating {@link HectorCassandraClient} instances.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class HectorCassandraClientConfigurer {

    protected String hosts;
    protected Integer port;
    protected String clusterName;
    protected String keyspaceName;
    protected Integer columnFamilyGcGraceSeconds;
    protected CassandraConsistencyLevel readConsistencyLevel;
    protected CassandraConsistencyLevel writeConsistencyLevel;

    /**
     * Cassandra hosts
     *
     * @param hosts Command delimited list of Cassandra hosts (hostnames or ipaddresses)
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer hosts(String hosts) {
        this.hosts = hosts;
        return this;
    }

    /**
     * Sets the port matching the host name of the Cassandra cluster to connect to.
     *
     * @param port The cassandra port. Set null to use the default port number {@link
     *             CassandraHost#DEFAULT_PORT}
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer port(Integer port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the keyspaceName of the keyspace to connect to.
     *
     * @param keyspaceName The keyspace name.
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer keyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    /**
     * (Optional) Sets the cluster name used internally by the hector library. Use this if you plan
     * to connect to more than one cassandra cluster in the same JVM.
     *
     * @param clusterName The cluster 'tag' name.
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * (Optional) Sets the gcGraceSeconds that will be used when creating column families for types.
     * If null will use default value
     *
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer columnFamilyGcGraceSeconds(Integer columnFamilyGcGraceSeconds) {
        this.columnFamilyGcGraceSeconds = columnFamilyGcGraceSeconds;
        return this;
    }

    /**
     * (Optional)
     *
     * @param readConsistencyLevel The consistencly level used for read by id operations. (default:
     *                             {@link CassandraConsistencyLevel#QUORUM})
     * @return {@code this} instance.
     */
    public HectorCassandraClientConfigurer readConsistencyLevel(CassandraConsistencyLevel readConsistencyLevel) {
        this.readConsistencyLevel = readConsistencyLevel;
        return this;
    }

    /**
     * (Optional)
     *
     * @param writeConsistencyLevel The consistencly level used for write/update/remove operations.
     *                              (default: {@link CassandraConsistencyLevel#QUORUM})
     * @return HectorCassandraClientConfigurer
     */
    public HectorCassandraClientConfigurer writeConsistencyLevel(CassandraConsistencyLevel writeConsistencyLevel) {
        this.writeConsistencyLevel = writeConsistencyLevel;
        return this;
    }

    /**
     * @return An instance of {@link HectorCassandraClient} matching this configurer configuration.
     */
    public HectorCassandraClient create() {

        if (hosts == null) {
            throw new IllegalArgumentException("hosts must be set");
        }

        if (!StringUtils.hasLength(hosts.replace(",", ""))) {
            throw new IllegalArgumentException("hosts cannot be null or empty");
        }

        if (port != null && port <= 0) {
            throw new IllegalArgumentException("port must be positive number");
        } else if (port == null) {
            port = CassandraHost.DEFAULT_PORT;
        }

        CassandraHostConfigurator config = new CassandraHostConfigurator();
        config.setHosts(hosts);
        config.setPort(port);

        return new HectorCassandraClient(
                config,
                keyspaceName,
                clusterName,
                columnFamilyGcGraceSeconds,
                readConsistencyLevel,
                writeConsistencyLevel);
    }

}
