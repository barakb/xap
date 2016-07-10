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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

@com.gigaspaces.api.InternalApi
public class FetchReliableAsyncMissingMemberDataPacket extends AbstractGroupNameReplicationPacket<ReliableAsyncMemberData> {
    private static final long serialVersionUID = 1L;

    private String _missingMemberName;

    public FetchReliableAsyncMissingMemberDataPacket() {
    }

    public FetchReliableAsyncMissingMemberDataPacket(String groupName,
                                                     String missingMemberName) {
        super(groupName);
        _missingMemberName = missingMemberName;
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeRepetitiveString(out, _missingMemberName);
    }

    @Override
    protected void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _missingMemberName = IOUtils.readRepetitiveString(in);
    }

    @Override
    public ReliableAsyncMemberData accept(
            IIncomingReplicationFacade incomingReplicationFacade) {
        IReplicationSourceGroup sourceGroup = incomingReplicationFacade.getReplicationSourceGroup(getGroupName());
        DynamicSourceGroupConfigHolder configHolder = sourceGroup.getConfigHolder();
        SourceGroupConfig config = configHolder.getConfig();
        if (!Arrays.asList(config.getMembersLookupNames()).contains(_missingMemberName))
            config = configHolder.getConfigWithMemberBeingAdded();
        if (config == null || !Arrays.asList(config.getMembersLookupNames()).contains(_missingMemberName)) {
            //Double check as the config could have been fully updated by now as we are not holding
            //any locks.
            config = configHolder.getConfig();
            if (!Arrays.asList(config.getMembersLookupNames()).contains(_missingMemberName))
                return null;
        }
        ReliableAsyncSourceGroupConfig reliableAsyncSourceGroupConfig = (ReliableAsyncSourceGroupConfig) config;
        IReplicationChannelDataFilter filter = config.getFilter(_missingMemberName);
        DynamicSourceGroupMemberLifeCycle lifeCycle = config.getLifeCycle(_missingMemberName);
        BacklogMemberLimitationConfig memberLimitations = config.getBacklogConfig().getBacklogMemberLimitation(_missingMemberName);
        AsyncChannelConfig asyncChannelConfig = reliableAsyncSourceGroupConfig.getChannelConfig(_missingMemberName);

        RouterStubHolder routerStubHolder = incomingReplicationFacade.getRemoteRouterStub(_missingMemberName);
        return new ReliableAsyncMemberData(memberLimitations, filter, lifeCycle, routerStubHolder, asyncChannelConfig);
    }

    public String getMissingMemberLookupName() {
        return _missingMemberName;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("missingMemberName", _missingMemberName);
    }

}
