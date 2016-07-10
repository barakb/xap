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

package com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel;

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncReplicationSourceChannel;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;

import java.util.LinkedList;
import java.util.List;

/**
 * This policy checks whether a group of replication members defined to operate in synchronous mode
 * are currently operating in synchrnous mode
 *
 * @author eitany
 * @since 9.5.1
 */
@com.gigaspaces.api.InternalApi
public class SyncMembersInSyncConsistencyLevelPolicy extends GroupConsistencyLevelPolicy {

    private int _minNumberOfSyncMembers = -1;

    public SyncMembersInSyncConsistencyLevelPolicy minNumberOfSyncMembers(int minNumberOfSyncMembers) {
        if (minNumberOfSyncMembers <= 1)
            throw new IllegalArgumentException("minNumberOfSyncMembers must be larger than 1");
        _minNumberOfSyncMembers = minNumberOfSyncMembers;
        return this;
    }

    public int getMinNumberOfSyncMembers() {
        return _minNumberOfSyncMembers;
    }

    public boolean isAllSyncMembersNeedsToBeInSync() {
        return _minNumberOfSyncMembers == -1;
    }

    public void checkConsistencyLevel(SyncReplicationSourceChannel[] syncChannels, String myMemberName) throws ConsistencyLevelViolationException {
        int numberOfInSyncMembers = 1;
        for (SyncReplicationSourceChannel syncChannel : syncChannels) {
            final ReplicationOperatingMode channelOpertingMode = syncChannel.getChannelOpertingMode();
            if (isAllSyncMembersNeedsToBeInSync()) {
                if (channelOpertingMode != ReplicationOperatingMode.SYNC)
                    throw new ConsistencyLevelViolationException(
                            "Replication member ["
                                    + syncChannel.getMemberName()
                                    + "] is not in synchronous state while consistency level policy states that all synchronous members must be in synchronous state");
            } else {
                if (channelOpertingMode == ReplicationOperatingMode.SYNC) {
                    if (++numberOfInSyncMembers >= getMinNumberOfSyncMembers())
                        return;
                }
            }
        }

        //We want to construct and message with description stating the member names which are not in sync, we do that only if previous logic
        //decided the sla is breached, this second iteration may result in sla not being breached due to change in state between
        //this two calls
        if (!isAllSyncMembersNeedsToBeInSync()) {
            List<String> insyncMembers = new LinkedList<String>();
            insyncMembers.add(myMemberName);
            List<String> outOfSyncMembers = new LinkedList<String>();
            numberOfInSyncMembers = 1;
            for (SyncReplicationSourceChannel syncChannel : syncChannels) {
                final ReplicationOperatingMode channelOpertingMode = syncChannel.getChannelOpertingMode();
                if (channelOpertingMode == ReplicationOperatingMode.SYNC) {
                    if (++numberOfInSyncMembers >= getMinNumberOfSyncMembers())
                        return;
                    insyncMembers.add(syncChannel.getMemberName());
                } else {
                    outOfSyncMembers.add(syncChannel.getMemberName());
                }
            }
            throw new ConsistencyLevelViolationException("The minimal number of synchronous members in synchronous state required by the consistency level policy is not met, ["
                    + insyncMembers.size()
                    + "/"
                    + (syncChannels.length + 1)
                    + "] members are currently in synchronous state while the policy requires at least "
                    + getMinNumberOfSyncMembers()
                    + " member to be in synchronous state. The members in synchronous state are "
                    + insyncMembers
                    + ". The members which are not in synchronous state are "
                    + outOfSyncMembers);
        }
    }

    public void checkConsistencyLevel(
            SyncReplicationSourceChannel syncChannel) {
        if (syncChannel.getChannelOpertingMode() != ReplicationOperatingMode.SYNC)
            throw new ConsistencyLevelViolationException(
                    "Replication member ["
                            + syncChannel.getMemberName()
                            + "] is not in synchronous state while consistency level policy states that it must be in synchronous state");
    }

}
