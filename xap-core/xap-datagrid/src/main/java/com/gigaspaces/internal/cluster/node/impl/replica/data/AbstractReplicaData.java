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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.internal.cluster.node.impl.replica.IExecutableSpaceReplicaData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.cluster.ReplicationFilterException;

public abstract class AbstractReplicaData implements IExecutableSpaceReplicaData {
    private static final long serialVersionUID = -5544026553791931067L;

    protected ITypeDesc getTypeDescriptor(SpaceTypeManager spaceTypeManager, IEntryPacket entryPacket) {
        IServerTypeDesc serverTypeDesc = null;
        final String className = entryPacket.getTypeName();
        if (className != null) {
            serverTypeDesc = spaceTypeManager.getServerTypeDesc(className);
            // can not be null since this entry was introduced already, maybe on
            // concurrent clean
            if (serverTypeDesc == null)
                try {
                    serverTypeDesc = spaceTypeManager.loadServerTypeDesc(entryPacket);
                } catch (Exception e) {
                    throw new ReplicationFilterException("", e);
                }
        }

        ITypeDesc typeDesc = serverTypeDesc == null ? null : serverTypeDesc.getTypeDesc();

        return typeDesc;
    }


}