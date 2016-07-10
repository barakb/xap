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

import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;

import net.jini.id.Uuid;

@com.gigaspaces.api.InternalApi
public class MemberProxyBasedReplicationMonitoredConnection<T, L>
        extends AbstractProxyBasedReplicationMonitoredConnection<T, L> {

    private final boolean _supportsConnectivityCheckEvents;

    public MemberProxyBasedReplicationMonitoredConnection(
            AbstractConnectionProxyBasedReplicationRouter<T, L> router,
            String targetLookupName,
            IReplicationConnectionProxy connectionProxy, T tag,
            String endpointLookupName, IConnectionMonitor<T, L> monitor, L url,
            ConnectionState state, Exception disconnectionReason, Uuid proxyId,
            IAsyncContextProvider asyncContextProvider,
            boolean supportsConnectivityCheckEvents) {
        super(router,
                targetLookupName,
                connectionProxy,
                tag,
                endpointLookupName,
                monitor,
                url,
                state,
                disconnectionReason,
                proxyId,
                asyncContextProvider);
        _supportsConnectivityCheckEvents = supportsConnectivityCheckEvents;
    }

    @Override
    protected void onClose() {
        getRouter().removeMemberConnection(this);
    }

    @Override
    public boolean supportsConnectivityCheckEvents() {
        return _supportsConnectivityCheckEvents;
    }

}
