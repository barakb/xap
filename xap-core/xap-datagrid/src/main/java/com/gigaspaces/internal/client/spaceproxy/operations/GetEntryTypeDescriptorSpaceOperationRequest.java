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
public class GetEntryTypeDescriptorSpaceOperationRequest extends SpaceOperationRequest<GetEntryTypeDescriptorSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private String _typeName;

    /**
     * Required for Externalizable
     */
    public GetEntryTypeDescriptorSpaceOperationRequest() {
    }

    public GetEntryTypeDescriptorSpaceOperationRequest(String typeName) {
        this._typeName = typeName;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("typeName", _typeName);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.GET_ENTRY_TYPE_DESCRIPTOR;
    }

    @Override
    public GetEntryTypeDescriptorSpaceOperationResult createRemoteOperationResult() {
        return new GetEntryTypeDescriptorSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return null;
    }

    @Override
    public boolean processPartitionResult(GetEntryTypeDescriptorSpaceOperationResult partitionResult,
                                          List<GetEntryTypeDescriptorSpaceOperationResult> previousResults, int numOfPartitions) {
        setRemoteOperationResult(partitionResult);
        return partitionResult.getTypeDesc() == null;
    }

    public ITypeDesc getFinalResult() throws Exception {
        GetEntryTypeDescriptorSpaceOperationResult result = getRemoteOperationResult();
        if (result == null)
            throw new IllegalStateException("Operation execution returned null result.");
        if (result.getExecutionException() != null)
            throw result.getExecutionException();
        return result.getTypeDesc();
    }

    public String getTypeName() {
        return _typeName;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "getTypeDescriptor";
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, _typeName);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this._typeName = IOUtils.readString(in);
    }
}
