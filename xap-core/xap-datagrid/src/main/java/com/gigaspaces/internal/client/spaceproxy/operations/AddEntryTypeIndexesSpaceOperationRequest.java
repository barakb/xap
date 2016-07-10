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
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.metadata.index.SpaceIndex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class AddEntryTypeIndexesSpaceOperationRequest extends SpaceOperationRequest<AddEntryTypeIndexesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private String _typeName;
    private SpaceIndex[] _indexes;
    private boolean _isFromGateway;

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.ADD_ENTRY_TYPE_INDEXES;
    }

    @Override
    public AddEntryTypeIndexesSpaceOperationResult createRemoteOperationResult() {
        return new AddEntryTypeIndexesSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return null;
    }

    public String getTypeName() {
        return _typeName;
    }

    public SpaceIndex[] getIndexes() {
        return _indexes;
    }

    public boolean isFromGateway() {
        return _isFromGateway;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "addIndex";
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("typeName", _typeName);
        textualizer.append("isFromGateway", _isFromGateway);
        textualizer.append("indexes", _indexes);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, _typeName);
        out.writeInt(_indexes.length);
        for (int i = 0; i < _indexes.length; i++)
            IOUtils.writeObject(out, _indexes[i]);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this._typeName = IOUtils.readString(in);
        int length = in.readInt();
        this._indexes = new SpaceIndex[length];
        for (int i = 0; i < length; i++)
            this._indexes[i] = IOUtils.readObject(in);
    }
}
