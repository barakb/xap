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

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.Textualizer;

import net.jini.core.lease.Lease;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterEntriesListenerSpaceOperationRequest extends SpaceOperationRequest<RegisterEntriesListenerSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private ITemplatePacket _templatePacket;
    private NotifyInfo _notifyInfo;
    private long _lease;

    private transient RegisterEntriesListenerSpaceOperationResult _finalResult;

    /**
     * Required for Externalizable
     */
    public RegisterEntriesListenerSpaceOperationRequest() {
    }

    public RegisterEntriesListenerSpaceOperationRequest(ITemplatePacket templatePacket, NotifyInfo notifyInfo, long lease) {
        _templatePacket = templatePacket;
        _notifyInfo = notifyInfo;
        _lease = lease;
    }

    @Override
    public RegisterEntriesListenerSpaceOperationResult createRemoteOperationResult() {
        return new RegisterEntriesListenerSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_templatePacket.getRoutingFieldValue() != null)
            return PartitionedClusterExecutionType.SINGLE;

        //needed for notify recovery - otherwise template will not be recovered from other partitions.
        //need to check if can be replaced with checking the routing value
        _notifyInfo.setBroadcast(true);
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _templatePacket.getRoutingFieldValue();
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.REGISTER_ENTRIES_LISTENER;
    }

    public ITemplatePacket getTemplatePacket() {
        return _templatePacket;
    }

    public NotifyInfo getNotifyInfo() {
        return _notifyInfo;
    }

    public long getLease() {
        return _lease;
    }


    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public boolean processPartitionResult(
            RegisterEntriesListenerSpaceOperationResult remoteOperationResult,
            List<RegisterEntriesListenerSpaceOperationResult> previousResults,
            int numOfPartitions) {
        // notify supports partial failure - 
        // if some of the partitions failed to register the notify template and some succeeded - the operation is successful
        // the operation fails only if all the partitions executions failed
        if (_finalResult == null || _finalResult.getEventRegistration() == null)
            _finalResult = remoteOperationResult;

        // handle normal result
        if (remoteOperationResult.getEventRegistration() != null)
            _finalResult.getEventRegistration().addSequenceNumber(remoteOperationResult.getEventRegistration());

        return true;
    }


    public RegisterEntriesListenerSpaceOperationResult getFinalResult() {
        return _finalResult == null ? getRemoteOperationResult()
                : _finalResult;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("template", _templatePacket);
        textualizer.append("lease", _lease);
        textualizer.append("notifyInfo", _notifyInfo);
    }

    private static final short FLAG_LEASE = 1 << 0;
    private static final long DEFAULT_LEASE = Lease.FOREVER;

    @Override
    public String getLRMIMethodTrackingId() {
        return "addEntriesListener";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        IOUtils.writeObject(out, _notifyInfo);
        if (flags != 0) {
            if (_lease != DEFAULT_LEASE)
                out.writeLong(_lease);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        this._templatePacket = IOUtils.readObject(in);
        this._notifyInfo = IOUtils.readObject(in);
        if (flags != 0) {
            if ((flags & FLAG_LEASE) != 0)
                this._lease = in.readLong();
        } else {
            this._lease = DEFAULT_LEASE;
        }
    }

    @Override
    public RemoteOperationRequest<RegisterEntriesListenerSpaceOperationResult> createCopy(int targetPartitionId) {
        RegisterEntriesListenerSpaceOperationRequest copy = (RegisterEntriesListenerSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._templatePacket = _templatePacket.clone();
        return copy;
    }

    private short buildFlags() {
        short flags = 0;

        if (_lease != DEFAULT_LEASE)
            flags |= FLAG_LEASE;

        return flags;
    }


}
