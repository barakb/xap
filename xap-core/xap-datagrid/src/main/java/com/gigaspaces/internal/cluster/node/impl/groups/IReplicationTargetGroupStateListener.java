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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;

/**
 * Note to implementers: The methods of this interface will be called under replication locks If A
 * listener wishes to shutdown the replication node, It is advised to spawn a new thread in order to
 * avoid possible deadlocks
 */
public interface IReplicationTargetGroupStateListener {
    /**
     * Called when a target channel failed handshake due to becoming out of sync
     *
     * @return true if the target channel is allowed to resync (and lose data due to
     * resynchronization)
     */
    boolean onTargetChannelOutOfSync(String groupName, String channelSourceLookupName, IncomingReplicationOutOfSyncException outOfSyncReason);

    /**
     * Called when a target channel received a notice that its backlog was dropped at the source
     */
    void onTargetChannelBacklogDropped(String groupName, String channelSourceLooString, IBacklogMemberState memberState);

    /**
     * Called when a new channel is being established, just before the channel is created to allow
     * custom validation logic. An exception will consider the channel creation is invalid, regular
     * return will be considered as valid connection
     */
    void onTargetChannelCreationValidation(String groupName,
                                           String sourceMemberName, Object sourceUniqueId);

    /**
     * Called when a target channel lost connection to its source
     */
    void onTargetChannelSourceDisconnected(String groupName,
                                           String sourceMemberName, Object sourceUniqueId);

    /**
     * Called when a target channel is connected/created
     */
    void onTargetChannelConnected(String groupName,
                                  String sourceMemberName, Object sourceUniqueId);
}
