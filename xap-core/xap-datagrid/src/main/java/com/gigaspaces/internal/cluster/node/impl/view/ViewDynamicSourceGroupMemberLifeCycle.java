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

package com.gigaspaces.internal.cluster.node.impl.view;

import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.MemberRemovedEvent;
import com.gigaspaces.internal.server.space.LocalViewRegistrations;
import com.gigaspaces.management.space.SpaceQueryDetails;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class ViewDynamicSourceGroupMemberLifeCycle extends DynamicSourceGroupMemberLifeCycle {
    private final LocalViewRegistrations _localViewRegistrations;
    private final Collection<SpaceQueryDetails> _queryDescriptions;
    private final ConnectionEndpointDetails _connectionDetails;

    public ViewDynamicSourceGroupMemberLifeCycle(LocalViewRegistrations localViewRegistrations,
                                                 Collection<SpaceQueryDetails> queryDescriptions, ConnectionEndpointDetails connectionDetails) {
        this._localViewRegistrations = localViewRegistrations;
        this._queryDescriptions = queryDescriptions;
        this._connectionDetails = connectionDetails;
    }

    @Override
    public void afterMemberAdded(MemberAddedEvent memberAddedEvent) {
        super.afterMemberAdded(memberAddedEvent);
        _localViewRegistrations.add(memberAddedEvent.getMemberName(), _connectionDetails, _queryDescriptions);
    }

    @Override
    public void afterMemberRemoved(MemberRemovedEvent memberRemovedEvent) {
        super.afterMemberRemoved(memberRemovedEvent);
        _localViewRegistrations.remove(memberRemovedEvent.getMemberName());
    }

    @Override
    public Object[] getConstructionArguments() {
        return new Object[]{_queryDescriptions, _connectionDetails};
    }
}
