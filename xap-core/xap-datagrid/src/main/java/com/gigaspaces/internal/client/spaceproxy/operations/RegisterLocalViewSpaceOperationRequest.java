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

import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.exceptions.PartitionedExecutionExceptionsCollection;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.management.space.SpaceQueryDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterLocalViewSpaceOperationRequest extends SpaceScatterGatherOperationRequest<RegisterLocalViewSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private transient List<ITemplatePacket>[] _allQueryPackets;
    private transient List<SpaceQueryDetails>[] _allQueryDescriptions;
    private List<ITemplatePacket> _queryPackets;
    private List<SpaceQueryDetails> _queryDescriptions;
    private RouterStubHolder _viewStub;
    private int _batchSize;
    private long _batchTimeout;
    private transient PartitionedExecutionExceptionsCollection _errors;


    /**
     * Required for Externalizable
     */
    public RegisterLocalViewSpaceOperationRequest() {
    }

    public RegisterLocalViewSpaceOperationRequest(List<ITemplatePacket>[] allQueryPackets, List<SpaceQueryDetails>[] allQueryDescriptions,
                                                  RouterStubHolder viewStub, int batchSize, long batchTimeout) {
        this._allQueryPackets = allQueryPackets;
        this._allQueryDescriptions = allQueryDescriptions;
        // Initialize templates in case space is not partitioned.
        if (allQueryPackets.length == 1) {
            this._queryPackets = allQueryPackets[0];
            this._queryDescriptions = allQueryDescriptions[0];
        }
        this._viewStub = viewStub;
        this._batchSize = batchSize;
        this._batchTimeout = batchTimeout;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.REGISTER_LOCAL_VIEW;
    }

    @Override
    public RegisterLocalViewSpaceOperationResult createRemoteOperationResult() {
        return new RegisterLocalViewSpaceOperationResult();
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
        return "registerLocalView";
    }

    public Collection<ITemplatePacket> getQueryPackets() {
        return _queryPackets;
    }

    public Collection<SpaceQueryDetails> getQueryDescriptions() {
        return _queryDescriptions;
    }

    public RouterStubHolder getViewStub() {
        return _viewStub;
    }

    public int getBatchSize() {
        return _batchSize;
    }

    public long getBatchTimeout() {
        return _batchTimeout;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _queryPackets);
        IOUtils.writeObject(out, _queryDescriptions);
        IOUtils.writeObject(out, _viewStub);
        out.writeInt(_batchSize);
        out.writeLong(_batchTimeout);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        _queryPackets = IOUtils.readObject(in);
        _queryDescriptions = IOUtils.readObject(in);
        _viewStub = IOUtils.readObject(in);
        _batchSize = in.readInt();
        _batchTimeout = in.readLong();
    }

    @Override
    public void scatterIndexesToPartitions(ScatterGatherOperationFutureListener<RegisterLocalViewSpaceOperationResult> scatterGatherCoordinator) {
        for (int partitionId = 0; partitionId < _allQueryPackets.length; partitionId++)
            if (!_allQueryPackets[partitionId].isEmpty())
                scatterGatherCoordinator.addPartition(partitionId, this);
    }

    @Override
    public void loadPartitionData(ScatterGatherRemoteOperationRequest<RegisterLocalViewSpaceOperationResult> mainRequest) {
        final int partitionId = getPartitionInfo().getPartitionId();
        this._queryPackets = ((RegisterLocalViewSpaceOperationRequest) mainRequest)._allQueryPackets[partitionId];
        this._queryDescriptions = ((RegisterLocalViewSpaceOperationRequest) mainRequest)._allQueryDescriptions[partitionId];
    }

    @Override
    public boolean processPartitionResult(ScatterGatherRemoteOperationRequest<RegisterLocalViewSpaceOperationResult> partitionRequest,
                                          List<ScatterGatherRemoteOperationRequest<RegisterLocalViewSpaceOperationResult>> previousRequests) {
        Exception partitionException = partitionRequest.getRemoteOperationResult().getExecutionException();
        if (partitionException != null) {
            if (_errors == null)
                _errors = new PartitionedExecutionExceptionsCollection();
            _errors.add(partitionRequest.getPartitionInfo().getPartitionId(), partitionException);
        }
        return true;
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
}
