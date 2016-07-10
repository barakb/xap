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

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilterBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.GatewayChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.view.ViewReplicationChannelDataFilter;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateHolderFactory;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.LeaseManager;

import net.jini.core.lease.Lease;

/**
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationChannelDataFilterBuilder
        implements IReplicationChannelDataFilterBuilder {

    private final SpaceEngine _spaceEngine;

    public SpaceReplicationChannelDataFilterBuilder(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
    }

    @Override
    public IReplicationChannelDataFilter createFilter(String name,
                                                      String groupName, Object... arguments) throws Exception {
        if (name.equals(ViewReplicationChannelDataFilter.class.getName())) {
            ITemplatePacket[] templates = new ITemplatePacket[arguments.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = (ITemplatePacket) arguments[i];
                _spaceEngine.getTypeManager().loadServerTypeDesc(templates[i]);
            }
            return new ViewReplicationChannelDataFilter(_spaceEngine.getCacheManager(), groupName, templates, _spaceEngine.getTemplateScanner().getRegexCache(), _spaceEngine.getTypeManager());
        }
        if (name.equals(NotificationReplicationChannelDataFilter.class.getName())) {
            ITemplatePacket template = (ITemplatePacket) arguments[0];
            NotifyInfo notifyInfo = (NotifyInfo) arguments[1];
            Long eventId = (Long) arguments[2];

            IServerTypeDesc typeDesc = _spaceEngine.getTypeManager().loadServerTypeDesc(template);

            NotifyTemplateHolder tHolder = TemplateHolderFactory.createNotifyTemplateHolder(typeDesc,
                    template,
                    template.getUID(),
                    LeaseManager.toAbsoluteTime(Lease.FOREVER),
                    eventId,
                    notifyInfo,
                    false /* isFifo */);

            return new NotificationReplicationChannelDataFilter(_spaceEngine.getCacheManager(),
                    groupName,
                    _spaceEngine.getTemplateScanner().getRegexCache(),
                    _spaceEngine.getTypeManager(),
                    _spaceEngine.getFilterManager(),
                    tHolder);
        }
        if (name.equals(GatewayChannelDataFilter.class.getName())) {
            //Support pre 9.5 creation of gateway filter, it didn't have any parameters
            boolean sendChangeAsUpdate = false;
            if (arguments != null && arguments.length == 1)
                sendChangeAsUpdate = (Boolean) arguments[0];

            return new GatewayChannelDataFilter(sendChangeAsUpdate);
        }

        throw new UnsupportedOperationException("Cannot create a filter with name " + name);
    }

}
