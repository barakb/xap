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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.exceptions.PartitionedExecutionExceptionsCollection;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class UnregisterLocalViewSpaceOperationRequest extends SpaceScatterGatherOperationRequest<UnregisterLocalViewSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private String _viewStubHolderName;
    private transient List<ITemplatePacket>[] _allTemplates;
    private transient PartitionedExecutionExceptionsCollection _errors;

    public UnregisterLocalViewSpaceOperationRequest() {
    }

    public UnregisterLocalViewSpaceOperationRequest(String viewStubHolderName, List<ITemplatePacket>[] allTemplates) {
        this._viewStubHolderName = viewStubHolderName;
        this._allTemplates = allTemplates;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.UNREGISTER_LOCAL_VIEW;
    }

    @Override
    public UnregisterLocalViewSpaceOperationResult createRemoteOperationResult() {
        return new UnregisterLocalViewSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.SCATTER_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        throw new IllegalStateException();
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "unregisterLocalView";
    }

    public String getViewStubHolderName() {
        return _viewStubHolderName;
    }

    public PartitionedExecutionExceptionsCollection getErrors() {
        if (getRemoteOperationResult() != null) {
            Exception executionException = getRemoteOperationResult().getExecutionException();
            if (executionException == null)
                return null;
            _errors = new PartitionedExecutionExceptionsCollection();
            _errors.add(0, executionException);
        }
        return _errors;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, _viewStubHolderName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        _viewStubHolderName = IOUtils.readString(in);
    }

    @Override
    public void scatterIndexesToPartitions(ScatterGatherOperationFutureListener<UnregisterLocalViewSpaceOperationResult> scatterGatherCoordinator) {
        for (int partitionId = 0; partitionId < _allTemplates.length; partitionId++)
            if (!_allTemplates[partitionId].isEmpty())
                scatterGatherCoordinator.addPartition(partitionId, this);
    }

    @Override
    public void loadPartitionData(ScatterGatherRemoteOperationRequest<UnregisterLocalViewSpaceOperationResult> mainRequest) {
        // Irrelevant for this operation.
    }

    @Override
    public boolean processPartitionResult(
            ScatterGatherRemoteOperationRequest<UnregisterLocalViewSpaceOperationResult> partitionRequest,
            List<ScatterGatherRemoteOperationRequest<UnregisterLocalViewSpaceOperationResult>> previousRequests) {
        Exception partitionException = partitionRequest.getRemoteOperationResult().getExecutionException();
        if (partitionException != null) {
            if (_errors == null)
                _errors = new PartitionedExecutionExceptionsCollection();
            _errors.add(partitionRequest.getPartitionInfo().getPartitionId(), partitionException);
        }
        return true;
    }
}
