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

package com.gigaspaces.internal.server.space.replication;

import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IDynamicSourceGroupMemberLifeCycleBuilder;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationDynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.view.ViewDynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.management.space.SpaceQueryDetails;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import java.util.Collection;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationLifeCycleBuilder
        implements IDynamicSourceGroupMemberLifeCycleBuilder {
    private final SpaceEngine _spaceEngine;

    public SpaceReplicationLifeCycleBuilder(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
    }

    @Override
    public DynamicSourceGroupMemberLifeCycle createLifeCycle(String name,
                                                             String groupName, Object... arguments) throws Exception {
        if (name.equals(NotificationDynamicSourceGroupMemberLifeCycle.class.getName())) {
            return new NotificationDynamicSourceGroupMemberLifeCycle(
                    _spaceEngine.getSpaceImpl(),
                    _spaceEngine.getFilterManager(),
                    null /* space context */);
        }

        if (name.equals(ViewDynamicSourceGroupMemberLifeCycle.class.getName())) {
            return new ViewDynamicSourceGroupMemberLifeCycle(_spaceEngine.getLocalViewRegistrations(),
                    (Collection<SpaceQueryDetails>) arguments[0],
                    (ConnectionEndpointDetails) arguments[1]);
        }
        throw new UnsupportedOperationException("Cannot create a lifeCycle with name " + name);
    }

}
