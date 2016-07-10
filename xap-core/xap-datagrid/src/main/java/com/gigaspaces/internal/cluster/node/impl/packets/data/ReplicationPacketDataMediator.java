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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationPacketDataMediator {
    private final Map<ServerTransaction, List<IReplicationTransactionalPacketEntryData>> _transactionPendingContent = new ConcurrentHashMap<ServerTransaction, List<IReplicationTransactionalPacketEntryData>>();

    public void setPendingTransactionData(
            ServerTransaction transaction, List<IReplicationTransactionalPacketEntryData> entriesData) {
        _transactionPendingContent.put(transaction, entriesData);
    }

    public List<IReplicationTransactionalPacketEntryData> removePendingTransactionData(ServerTransaction transaction) {
        return _transactionPendingContent.remove(transaction);
    }
}
