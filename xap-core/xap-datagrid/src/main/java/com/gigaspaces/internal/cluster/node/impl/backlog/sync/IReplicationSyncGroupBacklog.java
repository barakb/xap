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

package com.gigaspaces.internal.cluster.node.impl.backlog.sync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.ISyncReplicationGroupOutContext;
import com.gigaspaces.internal.server.storage.IEntryHolder;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;


public interface IReplicationSyncGroupBacklog extends IReplicationGroupBacklog {
    void add(ISyncReplicationGroupOutContext groupContext,
             IEntryHolder entryHolder, ReplicationSingleOperationType operationType);

    void addTransaction(ISyncReplicationGroupOutContext groupContext,
                        ServerTransaction transaction,
                        ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType);

    void addGeneric(ISyncReplicationGroupOutContext groupContext,
                    Object operationData,
                    ReplicationSingleOperationType operationType);


}
