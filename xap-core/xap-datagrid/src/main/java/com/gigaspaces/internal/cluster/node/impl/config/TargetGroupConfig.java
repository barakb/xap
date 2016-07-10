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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;


@com.gigaspaces.api.InternalApi
public class TargetGroupConfig
        extends GroupConfig {

    private final ProcessLogConfig _processLogConfig;
    private final ReplicationMode _groupChannelType;
    private int _channelCloseTimeout = 3;
    private boolean _unbounded;
    private boolean _supportDynamicMembers;

    public TargetGroupConfig(String groupName, ProcessLogConfig processLogConfig, ReplicationMode groupChannelType, String... membersLookupNames) {
        super(groupName, membersLookupNames);
        _processLogConfig = processLogConfig;
        _groupChannelType = groupChannelType;
    }

    public ProcessLogConfig getProcessLogConfig() {
        return _processLogConfig;
    }

    //Returns channel close timeout in seconds
    public int getChannelCloseTimeout() {
        return _channelCloseTimeout;
    }

    public void setChannelCloseTimeout(int channelCloseTimeout) {
        _channelCloseTimeout = channelCloseTimeout;
    }

    public void setUnbounded(boolean unbounded) {
        _unbounded = unbounded;
    }

    //If a group is unbounded we do not know the group member a-prior. Used for workaround to support
    //old mirror configuration with no cluster name specified
    public boolean isUnbounded() {
        return _unbounded;
    }

    public void setSupportDynamicMembers(boolean dynamicMembers) {
        _supportDynamicMembers = dynamicMembers;
    }

    public boolean isSupportDynamicMembers() {
        return _supportDynamicMembers;
    }

    public ReplicationMode getGroupChannelType() {
        return _groupChannelType;
    }

    public TargetGroupConfig duplicate(String groupName) {
        TargetGroupConfig duplicate = new TargetGroupConfig(groupName, getProcessLogConfig(), getGroupChannelType(), getMembersLookupNames());
        duplicate.setChannelCloseTimeout(getChannelCloseTimeout());
        duplicate.setHistoryLength(getHistoryLength());
        duplicate.setUnbounded(isUnbounded());
        duplicate.setSupportDynamicMembers(isSupportDynamicMembers());
        return duplicate;
    }

    @Override
    public String toString() {
        return "TargetGroupConfig [_processLogConfig=" + _processLogConfig
                + ", _channelCloseTimeout=" + _channelCloseTimeout +
                ", _unbounded=" + _unbounded + ", " +
                ", _supportDynamicMembers=" + _supportDynamicMembers + ", " +
                ", _groupChannelType=" + _groupChannelType +
                super.toString() + "]";
    }

}
