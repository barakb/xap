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


package com.j_spaces.core.server.transaction;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.0
 */

/**
 * contains all the per-entry transaction information. When there is no such information the object
 * is nulled
 */
@com.gigaspaces.api.InternalApi
public class EntryXtnInfo {
    //transaction that created the entry
//TBD- throw
    private XtnEntry _xidOriginated;

    //all the read lockers for this entry
    private ArrayList<XtnEntry> _readLocksOwners;

    //the write-locker for this entry
    private XtnEntry _writeLockOwner;

    //the type of write lock operation
    private int _writeLockOperation = SpaceOperations.NOOP;

    // ref to the shadow or master entry under updating xtn
    private IEntryHolder _otherUpdateUnderXtnEntry;

    /**
     * holds all entries waited by templates ReadIfExists/TaleIfExists
     */
    private Collection<ITemplateHolder> _waitingFor;


    public EntryXtnInfo() {

    }

    public EntryXtnInfo(EntryXtnInfo other) {//tbd
        _xidOriginated = other._xidOriginated;
        if (other._readLocksOwners != null && other._readLocksOwners.size() > 0) {
            _readLocksOwners = new ArrayList<XtnEntry>(other._readLocksOwners);
        }
        _writeLockOwner = other._writeLockOwner;
        _writeLockOperation = other._writeLockOperation;
        _otherUpdateUnderXtnEntry = other._otherUpdateUnderXtnEntry;
        _waitingFor = other._waitingFor;

    }

    /***
     * @return Returns ReadWriteLock Transaction's Owner Lists. <b>For internal use only.</b>
     **/
    public List<XtnEntry> getReadLocksOwners() {
        return _readLocksOwners;
    }

    /***
     * Create ReadWriteLock Transaction's Owner Lists if not created. This method is NOT thread
     * safe, should be called only if entry is locked or not shared. <b>For internal use only.</b>
     **/
    private List<XtnEntry> initReadLockOwnersIfNeed() {
        if (_readLocksOwners == null)
            _readLocksOwners = new ArrayList<XtnEntry>();

        return _readLocksOwners;
    }

    public void addReadLockOwner(XtnEntry xtn) {
        initReadLockOwnersIfNeed();

        if (!_readLocksOwners.contains(xtn))
            _readLocksOwners.add(xtn);
    }

    public boolean removeReadLockOwner(XtnEntry xtn) {
        boolean res = false;
        if (_readLocksOwners != null) {
            res = _readLocksOwners.remove(xtn);
            if (_readLocksOwners.isEmpty())
                _readLocksOwners = null;
        }
        return res;
    }

    public void clearReadLockOwners() {
        if (_readLocksOwners != null && _readLocksOwners.size() > 0)
            _readLocksOwners.clear();
        _readLocksOwners = null;
    }

    public boolean anyReadLockXtn() {
        return _readLocksOwners != null && _readLocksOwners.size() > 0;
    }

    /**
     * @param writeLockOwner the writeLockOwner to set
     */
    public void setWriteLockOwner(XtnEntry writeLockOwner) {
        _writeLockOwner = writeLockOwner;
    }

    public XtnEntry getWriteLockOwner() {
        return _writeLockOwner;
    }

    /**
     * @return the m_WriteLockOwner transaction
     */
    public ServerTransaction getWriteLockTransaction() {
        XtnEntry owner = _writeLockOwner;

        return owner == null ? null : owner.m_Transaction;
    }

    /**
     * @param xidOriginated the m_XidOriginated to set
     */
    public void setXidOriginated(XtnEntry xidOriginated) {
        this._xidOriginated = xidOriginated;
    }

    public XtnEntry getXidOriginated() {
        return _xidOriginated;
    }

    /**
     * @return the m_XidOriginated transaction
     */
    public ServerTransaction getXidOriginatedTransaction() {
        XtnEntry originated = _xidOriginated;
        return originated == null ? null : originated.m_Transaction;
    }

    public IEntryHolder getOtherUpdateUnderXtnEntry() {
        return _otherUpdateUnderXtnEntry;
    }

    public void setOtherUpdateUnderXtnEntry(IEntryHolder eh) {
        _otherUpdateUnderXtnEntry = eh;
    }

    /**
     * @return Returns the m_WaitingFor. StoredList that holds all entries waited by templates
     * ReadIfExists/TaleIfExists
     */
    public Collection<ITemplateHolder> getWaitingFor() {
        return _waitingFor;
    }

    public Collection<ITemplateHolder> initWaitingFor() {
        if (_waitingFor == null)
            _waitingFor = new HashSet<ITemplateHolder>();

        return _waitingFor;
    }


    public int getWriteLockOperation() {
        return _writeLockOperation;
    }

    public void setWriteLockOperation(int writeLockOperation) {
        _writeLockOperation = writeLockOperation;
    }

    public static EntryXtnInfo createCloneOrEmptyInfo(EntryXtnInfo original) {
        return original != null ? new EntryXtnInfo(original) : new EntryXtnInfo();
    }
}
