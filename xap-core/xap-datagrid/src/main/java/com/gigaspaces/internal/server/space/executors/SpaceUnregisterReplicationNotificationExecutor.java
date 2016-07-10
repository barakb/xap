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


import com.gigaspaces.internal.cluster.node.IReplicationNodeAdmin;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterReplicationNotificationRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceUnregisterReplicationNotificationExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space,
                                     SpaceRequestInfo spaceRequestInfo) {
        UnregisterReplicationNotificationRequestInfo requestInfo = (UnregisterReplicationNotificationRequestInfo) spaceRequestInfo;

        final IReplicationNodeAdmin replicationNodeAdmin = space.getEngine()
                .getReplicationNode()
                .getAdmin();

        String groupName = space.getEngine().generateGroupName();
        DynamicSourceGroupConfigHolder groupConfig = replicationNodeAdmin.getSourceGroupConfigHolder(groupName);

        groupConfig.removeMember(requestInfo.viewStubHolderName);
        // Register the view stub in the space replication node:
        replicationNodeAdmin.getRouterAdmin()
                .removeRemoteStubHolder(requestInfo.viewStubHolderName);

        return null;
    }
}
