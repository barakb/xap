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

package com.gigaspaces.internal.cluster.node.impl.notification;

import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.MemberRemovedEvent;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.core.filters.FilterOperationCodes;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class NotificationDynamicSourceGroupMemberLifeCycle extends DynamicSourceGroupMemberLifeCycle {
    private final SpaceImpl _space;
    private final SpaceContext _spaceContext;
    private final FilterManager _filterManager;

    private volatile NotifyTemplateHolder _tHolder;

    public NotificationDynamicSourceGroupMemberLifeCycle(
            SpaceImpl space,
            FilterManager filterManager,
            SpaceContext spaceContext) {
        _space = space;
        _filterManager = filterManager;
        _spaceContext = spaceContext;
    }

    @Override
    public void beforeMemberAdded(MemberAddedEvent memberAddedEvent) {
        NotificationReplicationChannelDataFilter channelFilter = (NotificationReplicationChannelDataFilter) memberAddedEvent.getFilter();

        _tHolder = channelFilter.getNotifyTemplateHolder();

        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_NOTIFY])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_NOTIFY, _spaceContext, _tHolder);

        channelFilter.initTemplate();

    }

    @Override
    public void afterMemberAdded(MemberAddedEvent memberAddedEvent) {
        // update durable notifications template counter
        _space.getEngine().getCacheManager().getTypeData(_tHolder.getServerTypeDesc()).incNumDurableNotificationsStored();
    }

    @Override
    public void afterMemberRemoved(MemberRemovedEvent memberRemovedEvent) {
        // update durable notifications template counter
        _space.getEngine().getCacheManager().getTypeData(_tHolder.getServerTypeDesc()).decNumDurableNotificationsStored();
    }
}
