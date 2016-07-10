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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder;

import com.gigaspaces.internal.cluster.node.impl.router.AbstractProxyBasedReplicationMonitoredConnection;

/**
 * Monitors {@link AbstractProxyBasedReplicationMonitoredConnection} state
 *
 * @author eitany
 * @see SpaceProxyReplicationRouter
 * @since 8.0
 */
public interface IConnectionMonitor<T, L> {
    /**
     * Adds a new connection to monitor
     *
     * @param connection connection to monitor
     */
    void monitor(AbstractProxyBasedReplicationMonitoredConnection<T, L> connection);

    /**
     * Update the monitor externally that the provided connection is disconnected Due to some call
     * dispatch on that proxy which throw a remote exception
     *
     * @param connection connection that was disconnected
     */
    void updateDisconnected(AbstractProxyBasedReplicationMonitoredConnection<T, L> connection, Exception reason);

    /**
     * Stops monitoring the specified connection
     *
     * @param connection that should be not be monitored anymore
     */
    void stopMonitoring(AbstractProxyBasedReplicationMonitoredConnection<T, L> connection);

    void close();

    String dumpState();

}
