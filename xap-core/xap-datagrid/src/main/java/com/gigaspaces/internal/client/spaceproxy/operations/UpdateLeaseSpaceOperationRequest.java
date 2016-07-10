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
import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.ObjectTypes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class UpdateLeaseSpaceOperationRequest extends SpaceOperationRequest<UpdateLeaseSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private String _uid;
    private String _typeName;
    private int _leaseObjectType;
    private long _duration;
    private transient Object _routingValue;
    private transient UpdateLeaseSpaceOperationResult _finalResult;

    /**
     * Required for Externalizable
     */
    public UpdateLeaseSpaceOperationRequest() {
    }

    public UpdateLeaseSpaceOperationRequest(String uid, String typeName, int leaseObjectType, long duration, Object routingValue) {
        this._uid = uid;
        this._typeName = typeName;
        this._leaseObjectType = leaseObjectType;
        this._duration = duration;
        this._routingValue = routingValue;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.UPDATE_LEASE;
    }

    @Override
    public UpdateLeaseSpaceOperationResult createRemoteOperationResult() {
        return new UpdateLeaseSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_routingValue != null)
            return PartitionedClusterExecutionType.SINGLE;

        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public boolean processPartitionResult(
            UpdateLeaseSpaceOperationResult remoteOperationResult,
            List<UpdateLeaseSpaceOperationResult> previousResults,
            int numOfPartitions) {
        if (remoteOperationResult.getExecutionException() != null) {
            _finalResult = remoteOperationResult;
            return false;
        }

        if (_finalResult == null)
            _finalResult = new UpdateLeaseSpaceOperationResult();

        return true;
    }

    public UpdateLeaseSpaceOperationResult getFinalResult() {
        return _finalResult == null ? getRemoteOperationResult() : _finalResult;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _routingValue;
    }

    public String getUid() {
        return _uid;
    }

    public String getTypeName() {
        return _typeName;
    }

    public int getLeaseObjectType() {
        return _leaseObjectType;
    }

    public long getDuration() {
        return _duration;
    }

    public boolean isCancel() {
        return _duration == LeaseUtils.DISCARD_LEASE;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("uid", _uid);
        textualizer.append("typeName", _typeName);
        textualizer.append("leaseObjectType", _leaseObjectType);
        textualizer.append("duration", _duration);
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return isCancel() ? "cancelLease" : "renewLease";
    }

    private static final short FLAG_UID = 1 << 0;
    private static final short FLAG_TYPE_NAME = 1 << 1;
    private static final short FLAG_LEASE_OBJECT_TYPE = 1 << 2;
    private static final short FLAG_DURATION = 1 << 3;

    private static final int DEFAULT_LEASE_OBJECT_TYPE = ObjectTypes.ENTRY;
    private static final long DEFAULT_DURATION = LeaseUtils.DISCARD_LEASE;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_uid != null)
                IOUtils.writeString(out, _uid);
            if (_typeName != null)
                IOUtils.writeString(out, _typeName);
            if (_leaseObjectType != DEFAULT_LEASE_OBJECT_TYPE)
                out.writeInt(_leaseObjectType);
            if (_duration != DEFAULT_DURATION)
                out.writeLong(_duration);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_UID) != 0)
                this._uid = IOUtils.readString(in);
            if ((flags & FLAG_TYPE_NAME) != 0)
                this._typeName = IOUtils.readString(in);
            this._leaseObjectType = (flags & FLAG_LEASE_OBJECT_TYPE) != 0 ? in.readInt() : DEFAULT_LEASE_OBJECT_TYPE;
            this._duration = (flags & FLAG_DURATION) != 0 ? in.readLong() : DEFAULT_DURATION;
        } else {
            this._leaseObjectType = DEFAULT_LEASE_OBJECT_TYPE;
            this._duration = DEFAULT_DURATION;
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_uid != null)
            flags |= FLAG_UID;
        if (_typeName != null)
            flags |= FLAG_TYPE_NAME;
        if (_leaseObjectType != DEFAULT_LEASE_OBJECT_TYPE)
            flags |= FLAG_LEASE_OBJECT_TYPE;
        if (_duration != DEFAULT_DURATION)
            flags |= FLAG_DURATION;

        return flags;
    }
}
