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

package com.gigaspaces.internal.cluster.node.impl.router;


/**
 * provides outgoing {@link IReplicationMonitoredConnection} and in charge of keeping it connected.
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationRouter {
    /**
     * Gets a replication connection to a designated replication node by its name, the received
     * connection is automatically managed by the router and it is in charge of detecting
     * disconnection and reestablish connection when available. Connection is established
     * asynchronously
     *
     * @param memberLookupName the replication node name to create the connection to
     * @return a connection to the specified replication node
     */
    IReplicationMonitoredConnection getMemberConnectionAsync(String memberLookupName);

    /**
     * Create new replication connection and establishes it.
     */
    IReplicationMonitoredConnection getMemberConnection(String memberLookupName);

    /**
     * Gets a connection with given custom url, need to have this due to space copy with given url
     * which may not even reside in the cluster policy and for sink recovery
     *
     * @param customUrl the url to the replication node to create the connection to
     * @return a connection to the specified replication node represented by the url
     */
    IReplicationMonitoredConnection getUrlConnection(Object customUrl);

    /**
     * Gets a direct connection to the router represented by the given stub.
     *
     * @param remoteStubHolder the target router of the connection
     * @return a connection to the specified router
     */
    IReplicationMonitoredConnection getDirectConnection(RouterStubHolder remoteStubHolder);

    void close();

    String getMyLookupName();

    Object getMyUniqueId();

    /**
     * Gets a stub holder for the current router, that a remote router can use to create a
     * connection to this router
     *
     * @see #getDirectConnection(RouterStubHolder)
     */
    RouterStubHolder getMyStubHolder();

    String dumpState();

    IReplicationRouterAdmin getAdmin();

    /**
     * Gets this router endpoint details
     */
    ReplicationEndpointDetails getMyEndpointDetails();

}
