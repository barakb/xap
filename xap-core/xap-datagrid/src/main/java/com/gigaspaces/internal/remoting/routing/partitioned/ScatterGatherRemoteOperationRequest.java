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

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public interface ScatterGatherRemoteOperationRequest<TResult extends RemoteOperationResult> extends RemoteOperationRequest<TResult> {
    void scatterIndexesToPartitions(ScatterGatherOperationFutureListener<TResult> scatterGatherCoordinator);

    void loadPartitionData(ScatterGatherRemoteOperationRequest<TResult> mainRequest);

    ScatterGatherPartitionInfo getPartitionInfo();

    boolean processPartitionResult(ScatterGatherRemoteOperationRequest<TResult> partitionRequest, List<ScatterGatherRemoteOperationRequest<TResult>> previousRequests);
}
