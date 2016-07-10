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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.lease.LeaseUpdateDetails;
import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class UpdateLeasesSpaceOperationRequest extends SpaceScatterGatherOperationRequest<UpdateLeasesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private LeaseUpdateDetails[] _leases;
    private boolean _isRenew;
    private transient Exception[] _errors;

    /**
     * Required for Externalizable
     */
    public UpdateLeasesSpaceOperationRequest() {
    }

    public UpdateLeasesSpaceOperationRequest(LeaseUpdateDetails[] leases, boolean isRenew) {
        this._leases = leases;
        this._isRenew = isRenew;
    }

    public int getSize() {
        return _leases.length;
    }

    public String getUid(int index) {
        return _leases[index].getUid();
    }

    public String getTypeName(int index) {
        return _leases[index].getTypeName();
    }

    public int getLeaseObjectType(int index) {
        return _leases[index].getLeaseObjectType();
    }

    public long getDurations(int index) {
        return _leases[index].getRenewDuration();
    }

    public Exception[] getFinalResult() {
        if (getRemoteOperationResult() != null) {
            final UpdateLeasesSpaceOperationResult result = getRemoteOperationResult();
            if (result.getErrors() != null)
                return result.getErrors();
            if (result.getExecutionException() != null) {
                _errors = new Exception[_leases.length];
                for (int i = 0; i < _errors.length; i++)
                    _errors[i] = result.getExecutionException();
            }
        }

        return _errors;
    }

    @Override
    public Object getAsyncFinalResult() throws Exception {
        return getFinalResult();
    }


    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.UPDATE_LEASES;
    }

    @Override
    public UpdateLeasesSpaceOperationResult createRemoteOperationResult() {
        return new UpdateLeasesSpaceOperationResult();
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
    public void scatterIndexesToPartitions(ScatterGatherOperationFutureListener<UpdateLeasesSpaceOperationResult> scatterGatherCoordinator) {
        for (int i = 0; i < _leases.length; i++) {
            int partitionId = scatterGatherCoordinator.getPartitionIdByHashcode(_leases[i].getRoutingValue());
            scatterGatherCoordinator.mapIndexToPartition(i, partitionId, this);
        }
    }

    @Override
    public void loadPartitionData(ScatterGatherRemoteOperationRequest<UpdateLeasesSpaceOperationResult> mainRequest) {
        UpdateLeasesSpaceOperationRequest main = (UpdateLeasesSpaceOperationRequest) mainRequest;
        this._leases = scatter(main._leases, new LeaseUpdateDetails[_partitionInfo.size()]);
    }

    @Override
    public boolean processPartitionResult(ScatterGatherRemoteOperationRequest<UpdateLeasesSpaceOperationResult> partitionRequest,
                                          List<ScatterGatherRemoteOperationRequest<UpdateLeasesSpaceOperationResult>> previousRequests) {
        // Otherwise, if this result contains an error, switch to partial failure mode processing:
        final UpdateLeasesSpaceOperationResult partitionResult = partitionRequest.getRemoteOperationResult();
        if (partitionResult.getExecutionException() != null) {
            // Initialize partial failure results if this is the first failure:
            if (_errors == null)
                _errors = new Exception[_leases.length];
            // Process current result:
            ((UpdateLeasesSpaceOperationRequest) partitionRequest).gather(_errors, partitionResult.getExecutionException());
        } else if (partitionResult.getErrors() != null) {
            // Initialize partial failure results if this is the first failure:
            if (_errors == null)
                _errors = new Exception[_leases.length];
            // Process current result:
            ((UpdateLeasesSpaceOperationRequest) partitionRequest).gather(_errors, partitionResult.getErrors());
        }

        return true;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("leases", _leases);
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "updateLeases";
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            writeExternalV9_7_0(out);
        else
            writeExternalOld(out);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            readExternalV9_7_0(in);
        else
            readExternalOld(in);
    }

    private void writeExternalV9_7_0(ObjectOutput out)
            throws IOException {
        out.writeBoolean(_isRenew);
        out.writeInt(_leases.length);
        for (int i = 0; i < _leases.length; i++) {
            IOUtils.writeString(out, _leases[i].getUid());
            IOUtils.writeString(out, _leases[i].getTypeName());
            out.writeInt(_leases[i].getLeaseObjectType());
            if (_isRenew)
                out.writeLong(_leases[i].getRenewDuration());
        }
    }

    private void readExternalV9_7_0(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._isRenew = in.readBoolean();
        this._leases = new LeaseUpdateDetails[in.readInt()];
        for (int i = 0; i < _leases.length; i++) {
            _leases[i] = new LeaseUpdateDetails();
            _leases[i].setUid(IOUtils.readString(in));
            _leases[i].setTypeName(IOUtils.readString(in));
            _leases[i].setLeaseObjectType(in.readInt());
            _leases[i].setRenewDuration(_isRenew ? in.readLong() : LeaseUtils.DISCARD_LEASE);
        }
    }

    private void writeExternalOld(ObjectOutput out)
            throws IOException {
        int length = _leases.length;
        out.writeInt(length);
        for (int i = 0; i < length; i++)
            IOUtils.writeString(out, _leases[i].getUid());

        out.writeInt(length);
        for (int i = 0; i < length; i++)
            IOUtils.writeString(out, _leases[i].getTypeName());

        out.writeInt(length);
        for (int i = 0; i < length; i++)
            out.writeInt(_leases[i].getLeaseObjectType());

        if (_isRenew) {
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeLong(_leases[i].getRenewDuration());
        } else
            out.writeInt(-1);
    }

    private void readExternalOld(ObjectInput in)
            throws IOException, ClassNotFoundException {
        int length = in.readInt();
        _leases = new LeaseUpdateDetails[length];
        for (int i = 0; i < length; i++) {
            _leases[i] = new LeaseUpdateDetails();
            _leases[i].setUid(IOUtils.readString(in));
        }

        length = in.readInt();
        for (int i = 0; i < length; i++)
            _leases[i].setTypeName(IOUtils.readString(in));

        length = in.readInt();
        for (int i = 0; i < length; i++)
            _leases[i].setLeaseObjectType(in.readInt());

        _isRenew = in.readInt() >= 0;
        if (_isRenew)
            for (int i = 0; i < length; i++)
                _leases[i].setRenewDuration(in.readLong());
    }
}
