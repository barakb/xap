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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.client.spaceproxy.operations.AddEntryTypeIndexesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.AddEntryTypeIndexesSpaceOperationResult;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class AddEntryTypeIndexesSpaceOperation extends AbstractSpaceOperation<AddEntryTypeIndexesSpaceOperationResult, AddEntryTypeIndexesSpaceOperationRequest> {
    @Override
    public void execute(AddEntryTypeIndexesSpaceOperationRequest request, AddEntryTypeIndexesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        ITypeDesc[] results = space.getEngine().getTypeManager().addIndexes(request.getTypeName(), request.getIndexes());
        result.setUpdatedTypeDescriptors(results);

        //replicate the new type
        IReplicationOutContext context = space.getEngine().getReplicationNode().createContext();
        try {
            // If operation was executed from a gateway, specify its from gateway
            if (request.isFromGateway()) {
                ((ReplicationOutContext) context).setFromGateway(true);
            }

            AddTypeIndexesRequestInfo requestInfo = new AddTypeIndexesRequestInfo(request.getTypeName(), request.getIndexes(), null);
            requestInfo.setFromGateway(request.isFromGateway());
            space.getEngine().getReplicationNode().outDataTypeAddIndex(context, requestInfo);
            space.getEngine().getReplicationNode().execute(context);
        } finally {
            context.release();
        }
    }

    @Override
    public String getLogName(AddEntryTypeIndexesSpaceOperationRequest request, AddEntryTypeIndexesSpaceOperationResult result) {
        return "add indexes";
    }
}
