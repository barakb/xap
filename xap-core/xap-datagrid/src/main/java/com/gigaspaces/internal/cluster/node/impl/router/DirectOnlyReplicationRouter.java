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

import com.gigaspaces.internal.cluster.node.impl.LRMIServiceExporter;

import net.jini.id.Uuid;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class DirectOnlyReplicationRouter extends AbstractConnectionProxyBasedReplicationRouter<Object, Object> {
    private DirectOnlyReplicationRouter(String myLookupName, Uuid uuid,
                                        IIncomingReplicationHandler incomingReplicationHandler) {
        super(myLookupName, uuid,
                new DummyConnectionMonitor(),
                new LRMIServiceExporter(),
                incomingReplicationHandler,
                new LRMIAsyncContextProvider(),
                true /*setMyIdBeforeDispatch*/);
    }

    @Override
    public IReplicationMonitoredConnection getUrlConnection(Object customUrl) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AbstractProxyBasedReplicationMonitoredConnection createNewMemberConnection(
            String lookupName, boolean connectSynchronously) {
        throw new UnsupportedOperationException();
    }

    public static class Builder extends ReplicationRouterBuilder<DirectOnlyReplicationRouter> {
        private final String _myLookupName;
        private final Uuid _uuid;

        public Builder(String myLookupName, Uuid uuid) {
            _myLookupName = myLookupName;
            _uuid = uuid;
        }

        @Override
        public DirectOnlyReplicationRouter create(IIncomingReplicationHandler handler) {
            return new DirectOnlyReplicationRouter(_myLookupName, _uuid, handler);
        }
    }
}
