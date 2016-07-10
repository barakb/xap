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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RemoteOperationsExecutorsClustersMonitor {
    private final RemoteOperationsExecutorsCluster[] _clusters;

    public RemoteOperationsExecutorsClustersMonitor(RemoteOperationsExecutorsCluster[] clusters, IAsyncHandlerProvider asyncHandlerProvider) {
        this._clusters = clusters;

        //asyncHandlerProvider.start(new ConnectedMembersMonitor(), settings.getMonitorFrequency(), "ConnectedMembersMonitor");
        //asyncHandlerProvider.start(new DisconnectedMembersMonitor(), settings.getDetectionFrequency(), "DisconnectedMembersMonitor");
    }

    public void close() {
    }

    private class ConnectedMembersMonitor extends AsyncCallable {
        @Override
        public CycleResult call() throws Exception {
            for (RemoteOperationsExecutorsCluster cluster : _clusters)
                cluster.refreshConnectedMembers();
            return CycleResult.IDLE_CONTINUE;
        }
    }

    private class DisconnectedMembersMonitor extends AsyncCallable {
        @Override
        public CycleResult call() throws Exception {
            for (RemoteOperationsExecutorsCluster cluster : _clusters)
                cluster.refreshDisconnectedMembers();
            return CycleResult.IDLE_CONTINUE;
        }
    }
}
