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


package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.client.LocalTransactionManager;
import com.j_spaces.core.fifo.FifoBackgroundRequest;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

import net.jini.core.transaction.server.ServerTransaction;

@com.gigaspaces.api.InternalApi
public class XtnEntry extends XtnInfo {
    private final Object _joinLock;
    public final transient XtnData _xtnData;
    private final transient FifoBackgroundRequest.AllowFifoNotificationsForNonFifoType _allowFifoNotificationsForNonFifoType;

    /**
     * Constructs a new Xtn Entry.
     */
    public XtnEntry(ServerTransaction xtn) {
        super(xtn);
        _xtnData = new XtnData(this);
        if (!(xtn.mgr instanceof LocalTransactionManager))
            _joinLock = new Object();
        else
            _joinLock = null;

        _allowFifoNotificationsForNonFifoType = new FifoBackgroundRequest.AllowFifoNotificationsForNonFifoType();
    }

    public Object getJoinLock() {
        return _joinLock;
    }

    /**
     * Returns true is new operations can still attach to this transaction, false otherwise
     * (transaction was already committed for example)
     *
     * @return true if XtnStatus UNINITIALIZED or BEGUN else false
     */
    public boolean isActive() {
        if (getStatus() == XtnStatus.UNINITIALIZED || getStatus() == XtnStatus.BEGUN)
            return true;

        return false;
    }

    public XtnStatus checkAndGetState_Granular() {
        //acquire the lock to make sure the transaction has begun
        XtnStatus status = getStatus();
        if (status == XtnStatus.UNINITIALIZED) {
            synchronized (getJoinLock()) {
                status = getStatus();
            }
        }
        return status;
    }

    public XtnData getXtnData() {
        return _xtnData;
    }

    public boolean anyFifoEntriesUnderXtn() {
        return getXtnData().anyFifoEntries();
    }

    public int getNumOfChangesUnderTransaction(CacheManager cacheManager) {

        int numOfChangesUnderTransaction = 0;
        IStoredList<IEntryCacheInfo> locked = _xtnData.getLockedEntries();

        if (locked != null && !locked.isEmpty()) {
            for (IStoredListIterator<IEntryCacheInfo> slh = locked.establishListScan(false); slh != null; slh = _xtnData.getLockedEntries().next(slh)) {
                IEntryCacheInfo pEntry = slh.getSubject();
                if (pEntry == null)
                    continue;
                IEntryHolder entryHolder = pEntry.getEntryHolder(cacheManager);

                if (entryHolder.isDeleted()
                        || entryHolder.getWriteLockOwner() != this
                        || entryHolder.getWriteLockOperation() == SpaceOperations.READ
                        || entryHolder.getWriteLockOperation() == SpaceOperations.READ_IE)
                    continue;

                numOfChangesUnderTransaction++;
            }
        }
        return numOfChangesUnderTransaction;
    }

    public FifoBackgroundRequest.AllowFifoNotificationsForNonFifoType getAllowFifoNotificationsForNonFifoEntries() {
        return _allowFifoNotificationsForNonFifoType;
    }
}
