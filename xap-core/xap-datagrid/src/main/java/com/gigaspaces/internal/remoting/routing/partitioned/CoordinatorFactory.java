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

package com.gigaspaces.internal.remoting.routing.partitioned;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;

@com.gigaspaces.api.InternalApi
public class CoordinatorFactory {
    public <T extends RemoteOperationResult> BroadcastOperationFutureListener<T> createAsyncBroadcastListener(
            RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener,
            PartitionedClusterRemoteOperationRouter router) {
        return createBroadcastListener(request, listener, router, true);
    }

    public <T extends RemoteOperationResult> BroadcastOperationFutureListener<T> createSyncBroadcastListener(
            RemoteOperationRequest<T> request, PartitionedClusterRemoteOperationRouter router) {
        return createBroadcastListener(request, null, router, false);
    }

    protected <T extends RemoteOperationResult> BroadcastOperationFutureListener<T> createBroadcastListener(RemoteOperationRequest<T> request,
                                                                                                            AsyncFutureListener<Object> listener,
                                                                                                            PartitionedClusterRemoteOperationRouter router,
                                                                                                            boolean getResultOnCompletion) {
        return new BroadcastOperationFutureListener<T>(request, listener, router, getResultOnCompletion);
    }

    public <T extends RemoteOperationResult> ScatterGatherOperationFutureListener<T> createAsyncScatterGatherListener(
            ScatterGatherRemoteOperationRequest<T> request, AsyncFutureListener<Object> listener,
            PartitionedClusterRemoteOperationRouter router) {
        return createScatterGatherListener(request, listener, router, true);
    }

    public <T extends RemoteOperationResult> ScatterGatherOperationFutureListener<T> createSyncScatterGatherListener(
            ScatterGatherRemoteOperationRequest<T> request, PartitionedClusterRemoteOperationRouter router) {
        return createScatterGatherListener(request, null, router, false);
    }

    protected <T extends RemoteOperationResult> ScatterGatherOperationFutureListener<T> createScatterGatherListener(
            ScatterGatherRemoteOperationRequest<T> request,
            AsyncFutureListener<Object> listener,
            PartitionedClusterRemoteOperationRouter router,
            boolean getResultOnCompletion) {
        return new ScatterGatherOperationFutureListener<T>(request, listener, router, getResultOnCompletion);
    }
}
