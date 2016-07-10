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

package com.j_spaces.core.cache.fifoGroup;

import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.storage.EntryHolder;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.XtnStatus;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * fifo groups  caching- used in order to preserve consistency of f-g when scanning
 * thru non f-g index or the problem of commmting after update which cause an entry to pop-up
 * as a head of f-g thru different matching template  
 */

@com.gigaspaces.api.InternalApi
public class FifoGroupsCacheHandler {
    private static class FgCacheElement {
        private final HashMap<Object, List<FgCacheElementEntryHolder>> _fgValueHolders;
        private final FgCacheElementEntryHolder _elementEntryHolderForRemoves; //in order to save recreations in case of remove
        private FgCacheElementEntryHolder _holdersPool;  //per segment pool
        private final IEntryHolder _dummyEH;
        private boolean _poolInitiated;

        private static final int HOLDERRS_PER_SEGMENT_POOL_SIZE = 5;

        public FgCacheElement() {
            //note- we assume that the size of the list is very small per value (generally 1)
            _fgValueHolders = new HashMap<Object, List<FgCacheElementEntryHolder>>();
            _elementEntryHolderForRemoves = new FgCacheElementEntryHolder();
            _dummyEH = new EntryHolder(null, null, 0, false, null); //for remove scans
        }

        private List<FgCacheElementEntryHolder> getFGEntriesPerValue(Object val) {
            return _fgValueHolders.get(val);
        }

        private void addToFGEntriesPerValue(Object val, IEntryHolder entry, XtnEntry xtn, List<FgCacheElementEntryHolder> entries, ITemplateHolder template) {
            if (!_poolInitiated) { //lazy initiation of holders pool
                _poolInitiated = true;

                for (int i = 0; i < HOLDERRS_PER_SEGMENT_POOL_SIZE; i++) {
                    FgCacheElementEntryHolder h = new FgCacheElementEntryHolder(null, null, true);
                    h.toPool(this);
                }
            }
            if (entries == null) {
                entries = new ArrayList<FgCacheElementEntryHolder>(1);
                _fgValueHolders.put(val, entries);
            }
            FgCacheElementEntryHolder h = _holdersPool != null ? FgCacheElementEntryHolder.fromPool(this) : new FgCacheElementEntryHolder();
            //perform shallow clone
            h.setValues(new EntryHolder(entry.getServerTypeDesc(), entry.getUID(), 0L, entry.isTransient(), entry.getTxnEntryData()), xtn, template);
            entries.add(h);
        }

        private void removeFromFGEntriesPerValue(Object val, String uid, XtnEntry xtn) {
            List<FgCacheElementEntryHolder> vals = _fgValueHolders.get(val);
            if (vals != null) {
                _dummyEH.setUID(uid);
                _elementEntryHolderForRemoves.setValues(_dummyEH, xtn, null);
                FgCacheElementEntryHolder cur = vals.remove(vals.indexOf(_elementEntryHolderForRemoves));
                _elementEntryHolderForRemoves.resetValues();
                if (cur._fromPool)
                    cur.toPool(this);
                if (vals.isEmpty())
                    _fgValueHolders.remove(val);

            }
        }


    }

    private static class FgCacheElementEntryHolder {
        private IEntryHolder _entry;
        private XtnEntry _xtn;
        private ITemplateHolder _holdingTemplate;
        private final boolean _fromPool;
        private FgCacheElementEntryHolder _next;

        public FgCacheElementEntryHolder() {
            this(null, null, false);
        }

        public FgCacheElementEntryHolder(IEntryHolder entry, XtnEntry xtn, boolean fromPool) {
            _entry = entry;
            _xtn = xtn;
            _fromPool = fromPool;
        }

        public void setValues(IEntryHolder entry, XtnEntry xtn, ITemplateHolder holdingTemplate) {
            _entry = entry;
            _xtn = xtn;
            _holdingTemplate = holdingTemplate;
        }

        public void resetValues() {
            _entry = null;
            _xtn = null;
            _holdingTemplate = null;
        }

        public void toPool(FgCacheElement ce) {
            _next = ce._holdersPool;
            resetValues();
            ce._holdersPool = this;
        }

        public static FgCacheElementEntryHolder fromPool(FgCacheElement ce) {
            FgCacheElementEntryHolder ans = ce._holdersPool;
            if (ans != null) {
                ce._holdersPool = ans._next;
                ans._next = null;
            }
            return ans;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            FgCacheElementEntryHolder other = (FgCacheElementEntryHolder) o;
            return _entry.getUID() == other._entry.getUID() && _xtn == other._xtn;
        }

        @Override
        public int hashCode() {
            return _entry.getUID().hashCode();
        }
    }

    private static class FifoGroupsLockManager {
        private static final int FIFO_GROUP_CACHE_FACTOR = 10;
        private static final int NUMBER_OF_FIFO_GROUPS_LOCKS = 1 << FIFO_GROUP_CACHE_FACTOR;
        private static final int CACHE_POS_COMPUTE = NUMBER_OF_FIFO_GROUPS_LOCKS - 1;
        private final FgCacheElement[] _locks;
        private final Object[] _indexLocks; //lock of f-g index in order to update scannable list of groups

        public FifoGroupsLockManager() {
            _locks = new FgCacheElement[NUMBER_OF_FIFO_GROUPS_LOCKS];
            _indexLocks = new Object[NUMBER_OF_FIFO_GROUPS_LOCKS];
            for (int i = 0; i < _locks.length; i++) {
                _locks[i] = new FgCacheElement();
                _indexLocks[i] = new Object();

            }
        }

        public FgCacheElement aquireLock(Object obj) {
            return _locks[obj.hashCode() & CACHE_POS_COMPUTE];
        }

        public void releaseLock(FgCacheElement lockObject) {
        }

        public Object aquireIndexLock(Object obj) {
            return _indexLocks[obj.hashCode() & CACHE_POS_COMPUTE];
        }

        public void releaseIndexLock(Object lockObject) {
        }


    }

    private final FifoGroupsLockManager _lm;

    private final CacheManager _cacheManager;

    public FifoGroupsCacheHandler(CacheManager cacheManager) {
        _lm = new FifoGroupsLockManager();
        _cacheManager = cacheManager;
    }

    //is this fg value + match unused ? if so set the entry as user
    boolean testAndSetFGCacheForEntry(Context context, IEntryHolder entry, ITemplateHolder template, Object val, boolean testOnly) {
        //save context fields
        final MatchResult matchResult = context.getLastMatchResult();
        final ITransactionalEntryData lastRawMatchSnapshot = context.getLastRawMatchSnapshot();
        final IEntryHolder lastRawmatchEntry = context.getLastRawmatchEntry();
        final ITemplateHolder lastRawmatchTemplate = context.getLastRawmatchTemplate();


        FgCacheElement lock = _lm.aquireLock(val);

        try {
            synchronized (lock) {
                List<FgCacheElementEntryHolder> entries = lock.getFGEntriesPerValue(val);
                if (entries != null) {
                    for (FgCacheElementEntryHolder e : entries) {
                        final XtnStatus entryWriteLockStatus = e._xtn.getStatus();
                        if (entryWriteLockStatus == XtnStatus.COMMITED ||
                                entryWriteLockStatus == XtnStatus.COMMITING ||
                                (entryWriteLockStatus == XtnStatus.PREPARED && e._xtn.m_SingleParticipant) ||
                                entryWriteLockStatus == XtnStatus.ROLLED
                                || (entryWriteLockStatus == XtnStatus.ROLLING && !e._xtn.m_AlreadyPrepared))
                            continue;

                        IEntryHolder curEntry = e._entry;
                        if (curEntry.isDeleted())
                            continue;
                        //is it the same F-G index ? if not- its not relevant
                        if (_cacheManager.getTypeData(entry.getServerTypeDesc()).getFifoGroupingIndex() !=
                                _cacheManager.getTypeData(curEntry.getServerTypeDesc()).getFifoGroupingIndex())
                            continue;

                        MatchResult match;
                        if (template.isEmptyTemplate() || template.getEntryData().getNumOfFixedProperties() <= curEntry.getEntryData().getNumOfFixedProperties()) {
                            //perfrom match between the requesting template and the already locked entry
                            match = template.match(_cacheManager, curEntry, -1 /*skipIndex*/, null, false /*safeEntry*/, context, _cacheManager.getEngine().getTemplateScanner().getRegexCache());
                            if (match == MatchResult.SHADOW || match == MatchResult.MASTER_AND_SHADOW ||
                                    (match == MatchResult.MASTER && context.getLastRawMatchSnapshot().getOtherUpdateUnderXtnEntry() == null)) {
                                if (template.getXidOriginated() != e._xtn)
                                    return false;
                                else
                                    continue;
                            }
                        }

                        if (e._holdingTemplate.isEmptyTemplate() || e._holdingTemplate.getEntryData().getNumOfFixedProperties() <= entry.getEntryData().getNumOfFixedProperties()) {
                            //perfrom match between the template already locked and the requesting entry
                            match = e._holdingTemplate.match(_cacheManager, entry, -1 /*skipIndex*/, null, true /*safeEntry*/, context, _cacheManager.getEngine().getTemplateScanner().getRegexCache());
                            if (match == MatchResult.SHADOW || match == MatchResult.MASTER_AND_SHADOW ||
                                    (match == MatchResult.MASTER && context.getLastRawMatchSnapshot().getOtherUpdateUnderXtnEntry() == null))
                                return false;  //the requesting entry belongs to the f-g of the already locked entry
                        }
                    }
                }
                if (!testOnly) {
                    //no match or no uids-add entry
                    lock.addToFGEntriesPerValue(val, entry, template.getXidOriginated(), entries, template);
                }
                return true;
            }
        } finally {
            if (lock != null)
                _lm.releaseLock(lock);
            //restore context fields
            context.setRawmatchResult(lastRawMatchSnapshot, matchResult, lastRawmatchEntry, lastRawmatchTemplate);
        }

    }


    void removeEntryFromFGCache(Context context, String uid, Object val, XtnEntry xtnEntry) {
        FgCacheElement lock = _lm.aquireLock(val);
        try {
            synchronized (lock) {
                lock.removeFromFGEntriesPerValue(val, uid, xtnEntry);
            }
        } finally {
            if (lock != null)
                _lm.releaseLock(lock);
        }

    }

    public Object aquireIndexLock(Object obj) {
        return _lm.aquireIndexLock(obj);
    }

    public void releaseIndexLock(Object lockObject) {
        _lm.releaseIndexLock(lockObject);
    }


}
