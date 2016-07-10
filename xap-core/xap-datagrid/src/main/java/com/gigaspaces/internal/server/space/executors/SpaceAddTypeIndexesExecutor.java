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

package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.AddTypeIndexesResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.security.authorities.SpaceAuthority;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceAddTypeIndexesExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        AddTypeIndexesRequestInfo requestInfo = (AddTypeIndexesRequestInfo) spaceRequestInfo;
        AddTypeIndexesResponseInfo responseInfo = new AddTypeIndexesResponseInfo();

        try {
            ITypeDesc[] result = space.getEngine().getTypeManager().addIndexes(requestInfo.getTypeName(), requestInfo.getIndexes());
            responseInfo.setUpdatedTypeDescriptors(result);

            //replicate the new type
            IReplicationOutContext context = space.getEngine().getReplicationNode().createContext();

            // If operation was executed from a gateway, specify its from gateway
            if (requestInfo.isFromGateway()) {
                ((ReplicationOutContext) context).setFromGateway(true);
            }

            // TODO DATASOURCE: refacotring of add indexes replication (as done with add type desc)
            space.getEngine()
                    .getCacheManager()
                    .getStorageAdapter()
                    .addIndexes(requestInfo.getTypeName(),
                            requestInfo.getIndexes());

            space.getEngine().getReplicationNode().outDataTypeAddIndex(context, requestInfo);
            space.getEngine().getReplicationNode().execute(context);
            context.release();
        } catch (SpaceMetadataException e) {
            responseInfo.setMetadataException(e);
        }

        return responseInfo;
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.ALTER;
    }
}
