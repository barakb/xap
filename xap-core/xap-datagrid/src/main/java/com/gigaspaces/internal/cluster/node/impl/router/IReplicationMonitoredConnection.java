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


public interface IReplicationMonitoredConnection extends IReplicationConnection {
    public void setConnectionStateListener(IConnectionStateListener listener);

    public ConnectionState getState();

    /**
     * @return the reason for the last disconnection if any disconnection occurred. The reason does
     * not reset when a connection is reestablished.
     */
    public Exception getLastDisconnectionReason();

    public void setConnectivityCheckListener(IConnectivityCheckListener listener);

    /**
     * @return whether {@link #setConnectivityCheckListener(IConnectivityCheckListener)} listener is
     * supported.
     */
    public boolean supportsConnectivityCheckEvents();

    /**
     * @since 9.0.0
     */
    Long getTimeOfDisconnection();

    /**
     * @since 9.7
     */
    public String dumpState();


}
