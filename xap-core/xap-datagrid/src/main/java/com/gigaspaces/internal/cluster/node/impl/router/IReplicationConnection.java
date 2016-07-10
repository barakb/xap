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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import java.rmi.RemoteException;

public interface IReplicationConnection {
    /**
     * @return the final endpoint associated to this connection lookup name
     */
    String getFinalEndpointLookupName();

    /**
     * @return the direct endpoint associated to this connection id, if delegators are used this
     * will point to the directly connected delegator unique id and not the final destination unique
     * id
     */
    Object getClosestEndpointUniqueId();

    /**
     * @return the direct endpoint associated to this connection address, if delegators are used
     * this will point to the directly connected delegator address and not the final destination
     * address
     */
    Object getClosestEndpointAddress();

    <T> T dispatch(AbstractReplicationPacket<T> packet) throws RemoteException;

    <T> AsyncFuture<T> dispatchAsync(AbstractReplicationPacket<T> packet) throws RemoteException;

    void close();

    Object getConnectionUrl();

    long getGeneratedTraffic();

    long getReceivedTraffic();

    /**
     * @return the direct endpoint associated to this connection logical version, if delegators are
     * used this will point to the directly connected delegator logical version and not the final
     * destination version
     * @since 8.0.3
     */
    PlatformLogicalVersion getClosestEndpointLogicalVersion();

    /**
     * @return the direct endpoint associated to this connection details
     * @since 9.5
     */
    ConnectionEndpointDetails getClosestEndpointDetails();
}
