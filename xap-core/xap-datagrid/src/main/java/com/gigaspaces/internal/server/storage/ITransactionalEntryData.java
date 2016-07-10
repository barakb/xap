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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.server.MutableServerEntry;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.Collection;
import java.util.List;

/**
 * Contains all the data (mutable) fields of the entry. when an entry is changed a new IEntryData is
 * created and attached to the IEntryHolder.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public interface ITransactionalEntryData extends IEntryData, MutableServerEntry {
    EntryXtnInfo getEntryXtnInfo();

    ITransactionalEntryData createCopyWithoutTxnInfo();

    ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime);

    ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfo);

    ITransactionalEntryData createCopyWithTxnInfo(int newVersionID, long newExpirationTime);

    ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex);

    ITransactionalEntryData createCopy(boolean cloneXtnInfo, IEntryData newEntryData, long newExpirationTime);

    ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID);

    ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime);

    boolean anyReadLockXtn();

    List<XtnEntry> getReadLocksOwners();

    void addReadLockOwner(XtnEntry xtn);

    void removeReadLockOwner(XtnEntry xtn);

    void clearReadLockOwners();

    XtnEntry getWriteLockOwner();

    void setWriteLockOwner(XtnEntry writeLockOwner);

    int getWriteLockOperation();

    void setWriteLockOperation(int writeLockOperation);

    ServerTransaction getWriteLockTransaction();

    IEntryHolder getOtherUpdateUnderXtnEntry();

    void setOtherUpdateUnderXtnEntry(IEntryHolder entryHolder);

    XtnEntry getXidOriginated();

    void setXidOriginated(XtnEntry xidOriginated);

    ServerTransaction getXidOriginatedTransaction();

    Collection<ITemplateHolder> getWaitingFor();

    void initWaitingFor();

    boolean isExpired();

    boolean isExpired(long limit);
}
