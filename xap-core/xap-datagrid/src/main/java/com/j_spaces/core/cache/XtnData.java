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

package com.j_spaces.core.cache;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.StoredListFactory;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@com.gigaspaces.api.InternalApi
public class XtnData {
    final private XtnEntry _xtnEntry;
    private volatile IStoredList<IEntryCacheInfo> _newEntries;
    private volatile IStoredList<IEntryCacheInfo> _lockedEntries;
    private volatile IStoredList<IEntryCacheInfo> _lockedFifoEntries;     //locked entries from fifo classes
    //contains original content of entries for commit-type notify
    private volatile IStoredList<IEntryCacheInfo> _needNotifyEntries;
    //entries taken under this xtn
    private volatile IStoredList<IEntryCacheInfo> _takenEntries;
    //_reWrittenEntries contains uids of entries taken & rewritten
    //under the xtn, operation is replaced by update
    private Map<String, String> _reWrittenEntries;
    private volatile IStoredList<TemplateCacheInfo> _RTTemplates;
    private volatile IStoredList<TemplateCacheInfo> _NTemplates;
    //if the xtn contains fifo entries, contain the serial xtn number
    private long _fifoXtnNumber = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;

    private volatile Map<String, OperationID> _entriesOperationIDs;
    //contains uids of updated entries unter this xtn + array of indicators in case of a relevant partial update Or
    // list of mutators in case of in-place updates
    private volatile HashMap<String, Object> _updatedEntries;
    //entries that should be scaned only for f-g. 
    private volatile Map<String, Object> _fifoGroupsEntries;
    private List<IEntryHolder> _entriesForFifoGroupScan;

    //in commit stage after prepare- the list of entries to replicate under xtn
    private List<IEntryHolder> _entriesForReplicationIn2PCommit;

    private static final boolean[] EMPTY_INDICATORS = new boolean[0];

    public XtnData(XtnEntry xtnEntry) {
        _xtnEntry = xtnEntry;
    }

    public boolean isLockedEntry(IEntryCacheInfo pEntry) {
        IStoredList<IEntryCacheInfo> lockedEntries = getLockedEntries();
        return lockedEntries != null && lockedEntries.contains(pEntry);
    }

    /**
     * given a uid, signal the entry as rewritten under the xtn
     *
     * @param uid - uid of entry to signal
     */
    public void signalRewrittenEntry(String uid) {
        if (_reWrittenEntries == null)
            _reWrittenEntries = new ConcurrentHashMap<String, String>();

        _reWrittenEntries.put(uid, uid);

    }

    /**
     * check weather an entry is rewritten under xtn
     *
     * @param uid - uid of entry to check
     * @return true is entry with this uid is rewritten under xtn
     */
    public boolean isReWrittenEntry(String uid) {
        return _reWrittenEntries != null && _reWrittenEntries.containsKey(uid);

    }

    /**
     * remove reWritten entry indication if exists
     *
     * @param uid -uid of entry
     */
    public void removeRewrittenEntryIndication(String uid) {
        if (_reWrittenEntries != null)
            _reWrittenEntries.remove(uid);
    }

    /**
     * get the relevant entries under the xtn according to selected type
     *
     * @param selectType - selected type of entries under the xtn, from com.j_spaces.core.sadapter.SelctType
     * @return IStoredList<PEntry>
     */
    public IStoredList<IEntryCacheInfo> getUnderXtnEntries(int selectType) {
        IStoredList<IEntryCacheInfo> entries = null;
        switch (selectType) {
            case SelectType.NEW_ENTRIES:
                entries = _newEntries;
                break;
            case SelectType.NEED_NOTIFY_ENTRIES:
                entries = _needNotifyEntries;
                break;
            case SelectType.TAKEN_ENTRIES:
                entries = _takenEntries;
                break;
            case SelectType.ALL_FIFO_ENTRIES:
                entries = _lockedFifoEntries;
                break;

            default:
                entries = _lockedEntries;
        }
        return entries;
    }

    public boolean anyFifoEntries() {
        IStoredList<IEntryCacheInfo> lockedFifoEntries = _lockedFifoEntries;
        return lockedFifoEntries != null && !lockedFifoEntries.isEmpty();
    }


    /**
     * @return the xtn
     */
    public ServerTransaction getXtn() {
        return _xtnEntry.m_Transaction;
    }

    /**
     * @return the xtn entry
     */
    public XtnEntry getXtnEntry() {
        return _xtnEntry;
    }

    public void addLockedEntry(IEntryCacheInfo pEntry, OperationID operationID, boolean fifo) {
        getLockedEntries(true).add(pEntry);
        if (fifo)
            getLockedFifoEntries(true).add(pEntry);

        if (operationID != null) {
            Map<String, OperationID> entriesOperationIDs = _entriesOperationIDs;
            if (entriesOperationIDs == null)
                _entriesOperationIDs = entriesOperationIDs = new Hashtable<String, OperationID>();// ConcurrentHashMap<String, OperationID>();

            entriesOperationIDs.put(pEntry.getUID(), operationID);

        }
    }

    /**
     * When user operation under transaction is upgraded from read to update/take, the order of
     * entry should be changed to the most recent, so that the changes in prepare come in the right
     * order. For example:
     *
     * read object A read object B take object B take object A
     *
     * will cause a prepare of: B,A instead of A,B.
     *
     * Repeatable read doesn't cause the upgrade since it is not reflected in prepare.
     */
    public void updateLock(IEntryCacheInfo pEntry, OperationID operationID, boolean isReadOperation, boolean fifo) {
        // new entries and read operations order is not changed
        IStoredList<IEntryCacheInfo> newEntries = !isReadOperation ? getNewEntries() : null;
        if (!isReadOperation && (newEntries == null || !newEntries.contains(pEntry))) {
            // update entry ordering 
            IStoredList<IEntryCacheInfo> lockedEntries = getLockedEntries(true);
            lockedEntries.removeByObject(pEntry);
            lockedEntries.add(pEntry);
            if (fifo) {
                IStoredList<IEntryCacheInfo> lockedFifoEntries = getLockedFifoEntries(true);

                lockedFifoEntries.removeByObject(pEntry);
                lockedFifoEntries.add(pEntry);
            }

        }

        if (operationID != null) {
            Map<String, OperationID> entriesOperationIDs = _entriesOperationIDs;
            if (entriesOperationIDs == null)
                _entriesOperationIDs = entriesOperationIDs = new Hashtable<String, OperationID>();// ConcurrentHashMap<String, OperationID>();
            entriesOperationIDs.put(pEntry.getUID(), operationID);
        }

    }

    public OperationID getOperationID(String uid) {
        Map<String, OperationID> entriesOperationIDs = _entriesOperationIDs;
        return entriesOperationIDs != null ? entriesOperationIDs.get(uid) : null;
    }

    public void setUpdatedEntry(IEntryHolder eh, boolean[] partialUpdateIndicators) {
        HashMap<String, Object> updatedEntries = _updatedEntries;
        if (updatedEntries == null)
            _updatedEntries = updatedEntries = new HashMap<String, Object>();

        boolean[] partialUpdateIndicatorsToUse = EMPTY_INDICATORS;
        if (partialUpdateIndicators != null) {
            Object curPartialUpdateInfo = updatedEntries.get(eh.getUID());
            //a real indicators array, put it only if its the only update under this xtn
            boolean[] curPartialUpdateIndicators = EMPTY_INDICATORS;
            if (curPartialUpdateInfo != null && (curPartialUpdateInfo instanceof boolean[]))
                //if the prev update was in-place update consider the whole op as
                //regular update
                curPartialUpdateIndicators = (boolean[]) curPartialUpdateInfo;


            boolean any_partial = false;
            if (curPartialUpdateInfo == null) {
                partialUpdateIndicatorsToUse = partialUpdateIndicators;
            } else if (curPartialUpdateIndicators != EMPTY_INDICATORS && partialUpdateIndicators.length == curPartialUpdateIndicators.length) {
                partialUpdateIndicatorsToUse = curPartialUpdateIndicators;
                //more than one update under xtn for this entry, derive a consolidated indicators array
                for (int i = 0; i < partialUpdateIndicators.length; i++) {
                    partialUpdateIndicatorsToUse[i] = partialUpdateIndicatorsToUse[i] && partialUpdateIndicators[i];
                    any_partial = any_partial || partialUpdateIndicatorsToUse[i];
                }
                if (!any_partial)
                    partialUpdateIndicatorsToUse = EMPTY_INDICATORS;
            }
        }
        updatedEntries.put(eh.getUID(), partialUpdateIndicatorsToUse);
    }


    public void setInPlaceUpdatedEntry(IEntryHolder eh, Collection<SpaceEntryMutator> mutators) {
        HashMap<String, Object> updatedEntries = _updatedEntries;
        if (updatedEntries == null)
            _updatedEntries = updatedEntries = new HashMap<String, Object>();

        Object updateInfoToUse = mutators;
        Object curInfo = updatedEntries.get(eh.getUID());
        if (curInfo != null && !(curInfo instanceof List)) {
            updateInfoToUse = EMPTY_INDICATORS;

        } else {
            if (curInfo != null) {
                Collection<SpaceEntryMutator> curMutators = (Collection<SpaceEntryMutator>) curInfo;
                //integrate the mutators
                for (SpaceEntryMutator sm : mutators) {
                    curMutators.add(sm);
                }
                return;
            }
        }
        updatedEntries.put(eh.getUID(), updateInfoToUse);
    }


    public boolean isUpdatedEntry(IEntryHolder eh) {
        HashMap<String, Object> updatedEntries = _updatedEntries;
        return (updatedEntries != null && updatedEntries.containsKey(eh.getUID()));

    }

    public HashMap<String, Object> getUpdatedEntries() {
        return _updatedEntries;
    }

    public void addToEntriesForFifoGroupScan(IEntryHolder entry) { //we need to perform shallow clone
        if (_entriesForFifoGroupScan == null)
            _entriesForFifoGroupScan = new ArrayList<IEntryHolder>();

        _entriesForFifoGroupScan.add(entry.createCopy());
    }

    public List<IEntryHolder> getEntriesForFifoGroupScan() {
        return _entriesForFifoGroupScan;
    }

    public void resetEntriesForFifoGroupsScan() {
        _entriesForFifoGroupScan = null;
    }

    public boolean anyEntriesForFifoGroupScan() {
        return _entriesForFifoGroupScan != null && !_entriesForFifoGroupScan.isEmpty();
    }

    public void addToFifoGroupsEntries(IEntryHolder eh, Object groupValue) {
        Map<String, Object> fifoGroupsEntries = _fifoGroupsEntries;
        if (fifoGroupsEntries == null)
            _fifoGroupsEntries = fifoGroupsEntries = new HashMap<String, Object>();

        fifoGroupsEntries.put(eh.getUID(), groupValue);
    }

    public Map<String, Object> getFifoGroupsEntries() {
        return _fifoGroupsEntries;
    }

    public boolean anyFifoGroupOperations() {
        Map<String, Object> fifoGroupsEntries = _fifoGroupsEntries;
        return fifoGroupsEntries != null && !fifoGroupsEntries.isEmpty();
    }

    public IStoredList<IEntryCacheInfo> getLockedEntries() {

        return getLockedEntries(false);
    }

    public IStoredList<IEntryCacheInfo> getLockedEntries(boolean createIfNull) {
        IStoredList<IEntryCacheInfo> res = _lockedEntries;
        if (createIfNull && res == null)
            _lockedEntries = res = StoredListFactory.createHashList();
        return res;


    }

    public boolean anyLockedEntries() {
        IStoredList<IEntryCacheInfo> res = _lockedEntries;
        return res != null && !res.isEmpty();
    }

    public void setFifoXtnNumber(long fifoXtnNumber) {
        _fifoXtnNumber = fifoXtnNumber;
    }

    public long getFifoXtnNumber() {
        return _fifoXtnNumber;
    }

    public IStoredList<IEntryCacheInfo> getLockedFifoEntries() {
        return getLockedFifoEntries(false);
    }

    public IStoredList<IEntryCacheInfo> getLockedFifoEntries(boolean createIfNull) {
        IStoredList<IEntryCacheInfo> locked = _lockedFifoEntries;
        if (createIfNull && locked == null)
            _lockedFifoEntries = locked = StoredListFactory.createHashList();
        return locked;
    }

    public IStoredList<IEntryCacheInfo> getNeedNotifyEntries() {
        return getNeedNotifyEntries(false);
    }

    public IStoredList<IEntryCacheInfo> getNeedNotifyEntries(boolean createIfNull) {
        IStoredList<IEntryCacheInfo> res = _needNotifyEntries;

        if (createIfNull && res == null)
            _needNotifyEntries = res = StoredListFactory.createList(false);
        return res;
    }


    public IStoredList<IEntryCacheInfo> getNewEntries() {
        return getNewEntries(false /*createIfNull*/);
    }

    public IStoredList<IEntryCacheInfo> getNewEntries(boolean createIfNull) {
        IStoredList<IEntryCacheInfo> res = _newEntries;
        if (createIfNull && res == null)
            _newEntries = res = StoredListFactory.createHashList();
        return res;
    }

    public void removeFromNewEntries(IEntryCacheInfo subject) {
        IStoredList<IEntryCacheInfo> newEntries = getNewEntries();
        if (newEntries != null)
            newEntries.removeByObject(subject);
    }

    public IStoredList<IEntryCacheInfo> getTakenEntries() {
        return getTakenEntries(false);
    }

    public IStoredList<IEntryCacheInfo> getTakenEntries(boolean createIfNull) {
        IStoredList<IEntryCacheInfo> res = _takenEntries;
        if (createIfNull && res == null)
            _takenEntries = res = StoredListFactory.createHashList();
        return res;

    }

    public void addToTakenEntriesIfNotInside(IEntryCacheInfo entry) {
        IStoredList<IEntryCacheInfo> taken = getTakenEntries(true);
        if (!taken.contains(entry)) {
            taken.add(entry);
        }
    }

    public void removeTakenEntry(IEntryCacheInfo entry) {
        IStoredList<IEntryCacheInfo> taken = getTakenEntries();
        if (taken != null)
            taken.removeByObject(entry);
    }

    public IStoredList<TemplateCacheInfo> getNTemplates() {
        return _NTemplates;
    }

    public IStoredList<TemplateCacheInfo> getNTemplates(boolean createIfNull) {
        IStoredList<TemplateCacheInfo> res = _NTemplates;
        if (createIfNull && res == null)
            _NTemplates = res = StoredListFactory.createList(false);
        return res;

    }

    public IStoredList<TemplateCacheInfo> getRTTemplates() {
        return getRTTemplates(false);
    }

    public IStoredList<TemplateCacheInfo> getRTTemplates(boolean createIfNull) {
        IStoredList<TemplateCacheInfo> res = _RTTemplates;
        if (createIfNull && res == null)
            _RTTemplates = res = StoredListFactory.createList(false);
        return res;
    }

    public void setEntriesForReplicationIn2PCommit(List<IEntryHolder> pLocked) {
        _entriesForReplicationIn2PCommit = pLocked;
    }

    public List<IEntryHolder> getEntriesForReplicationIn2PCommit() {
        return _entriesForReplicationIn2PCommit;
    }


}