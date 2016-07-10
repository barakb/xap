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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl.IDirectPersistencyIoHandler;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The synchronizing direct-persistency handler
 *
 * @author yechielf
 * @since 10.2
 */
public interface IDirectPersistencySyncHandler {

    //should be called after blobstore/direct-persistency is initialized
    void initialize();

    long getCurrentGenerationId();

    boolean isOverflow();

    void beforeDirectPersistencyOp(IReplicationOutContext context, IEntryHolder entryHolder, boolean phantom);

    void beforeDirectPersistencyOp(IReplicationOutContext context, List<String> uids, Set<String> phantoms, Map<String, IEntryHolder> entryHolderMap);

    /* get the prev generation entries- an iterator of uids*/
    Iterator<String> getEntriesForRecovery();

    void afterInsertingToRedolog(IReplicationOutContext context, long redoKey);

    void afterOperationPersisted(IDirectPersistencyOpInfo e);

    void afterRecovery();

    void setBackLog(IReplicationGroupBacklog backlog);

    IReplicationGroupBacklog getBackLog();

    DirectPersistencyListHandler getListHandler();

    long getLastConfirmed();

    void setLastConfirmed(long confirmed);

    void close();

    SpaceEngine getSpaceEngine();

    //--------------- EMBEDDED LIST RELATED -------------------------------------
    boolean isEmbeddedListUsed();

    void afterInitializedBlobStoreIO(IDirectPersistencyIoHandler ioHandler);

    void onEmbeddedListRecordTransferStart(IDirectPersistencyOpInfo o, boolean onlyIfNotExists);

    void onEmbeddedListRecordTransferEnd(IDirectPersistencyOpInfo o);

    void onEmbeddedOpFromInitialLoad(String uid, long gen, long seq, boolean phantom);

    void onEmbeddedOpFromInitialLoad(List<String> uids, long gen, long seq, Set<String> phantoms);

    EmbeddedSyncHandler getEmbeddedSyncHandler();

}
