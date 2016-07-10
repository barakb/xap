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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;


@com.gigaspaces.api.InternalApi
public class SyncReplicationTargetChannel
        extends AbstractReplicationTargetChannel {

    public SyncReplicationTargetChannel(TargetGroupConfig groupConfig, String myLookupName,
                                        Object myUniqueId,
                                        ReplicationEndpointDetails sourceEndpointDetails,
                                        IReplicationMonitoredConnection sourceConnection,
                                        String groupName, IReplicationSyncTargetProcessLog processLog,
                                        IReplicationInFilter inFilter,
                                        IReplicationTargetGroupStateListener groupStateListener,
                                        IReplicationGroupHistory groupHistory) {
        super(groupConfig,
                myLookupName,
                myUniqueId,
                sourceEndpointDetails,
                sourceConnection,
                groupName,
                processLog,
                inFilter,
                groupStateListener,
                groupHistory,
                false);
    }

}
