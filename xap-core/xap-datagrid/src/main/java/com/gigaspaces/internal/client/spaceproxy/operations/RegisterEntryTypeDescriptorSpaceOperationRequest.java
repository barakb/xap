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
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterEntryTypeDescriptorSpaceOperationRequest extends SpaceOperationRequest<RegisterEntryTypeDescriptorSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private ITypeDesc _typeDesc;
    private boolean _gatewayProxy;

    /**
     * Required for Externalizable
     */
    public RegisterEntryTypeDescriptorSpaceOperationRequest() {
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("typeName", _typeDesc.getTypeName());
        textualizer.append("checksum", _typeDesc.getChecksum());
        textualizer.append("gatewayProxy", _gatewayProxy);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.REGISTER_ENTRY_TYPE_DESCRIPTOR;
    }

    @Override
    public RegisterEntryTypeDescriptorSpaceOperationResult createRemoteOperationResult() {
        return new RegisterEntryTypeDescriptorSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return null;
    }

    public ITypeDesc getTypeDesc() {
        return _typeDesc;
    }

    public RegisterEntryTypeDescriptorSpaceOperationRequest setTypeDesc(ITypeDesc typeDesc) {
        this._typeDesc = typeDesc;
        return this;
    }

    public boolean isGatewayProxy() {
        return _gatewayProxy;
    }

    public RegisterEntryTypeDescriptorSpaceOperationRequest setGatewayProxy(boolean gatewayProxy) {
        this._gatewayProxy = gatewayProxy;
        return this;
    }

    @Override
    public boolean processPartitionResult(RegisterEntryTypeDescriptorSpaceOperationResult partitionResult,
                                          List<RegisterEntryTypeDescriptorSpaceOperationResult> previousResults, int numOfPartitions) {
        if (partitionResult.hasException()) {
            setRemoteOperationResult(partitionResult);
            return false;
        }
        return true;
    }

    public void processExecutionException() throws Exception {
        if (getRemoteOperationResult() != null && getRemoteOperationResult().hasException())
            throw getRemoteOperationResult().getExecutionException();
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "registerTypeDescriptor";
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _typeDesc);
        out.writeBoolean(_gatewayProxy);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this._typeDesc = IOUtils.readObject(in);
        this._gatewayProxy = in.readBoolean();
    }
}
