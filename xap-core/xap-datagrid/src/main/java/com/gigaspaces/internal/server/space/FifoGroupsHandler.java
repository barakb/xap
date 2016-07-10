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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.EntryDeletedException;
import com.j_spaces.core.TemplateDeletedException;
import com.j_spaces.core.TransactionConflictException;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.fifoGroup.IFifoGroupIterator;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.locks.ILockObject;

import net.jini.core.transaction.TransactionException;

import java.util.Collection;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class FifoGroupsHandler {
    private final SpaceEngine _spaceEngine;
    private final CacheManager _cacheManager;

    public FifoGroupsHandler(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
        _cacheManager = _spaceEngine.getCacheManager();
    }


    IEntryHolder getMatchedEntryAndOperateSA_Scan(Context context,
                                                  ITemplateHolder template, boolean makeWaitForInfo,
                                                  boolean useSCN, final IScanListIterator<IEntryCacheInfo> toScan)
            throws TransactionException, TemplateDeletedException,
            SAException {
        long leaseFilter = SystemTime.timeMillis();
        boolean needMatch = !toScan.isAlreadyMatched();
        int alreadyMatchedFixedPropertyIndexPos = toScan.getAlreadyMatchedFixedPropertyIndexPos();
        String alreadyMatchedIndexPath = toScan.getAlreadyMatchedIndexPath();

        try {
            while (toScan.hasNext()) {
                IEntryCacheInfo pEntry = toScan.next();
                if (pEntry == null)
                    continue;
                IEntryHolder res = _spaceEngine.getMatchedEntryAndOperateSA_Entry(context,
                        template, makeWaitForInfo,
                        needMatch, alreadyMatchedFixedPropertyIndexPos, alreadyMatchedIndexPath, leaseFilter,
                        useSCN, pEntry);
                if (res != null)
                    return res;
                if (context.isFifoGroupScanEncounteredXtnConflict()) {
                    context.setFifoGroupScanEncounteredXtnConflict(false);
                    if (toScan.isIterator())
                        ((IFifoGroupIterator) toScan).nextGroup();
                }
            }
            return null;
        }//try
        finally {
            // scan ended, release resource
            toScan.releaseScan();
            if (context.isPendingExpiredEntriesExist() && _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates()) {
                try {
                    _spaceEngine.getLeaseManager().forceLeaseReaperCycle(false);
                } catch (InterruptedException e) {
                }
                context.setPendingExpiredEntriesExist(false);
            }
        }

    }


    void getMatchedEntriesAndOperateSA_Scan(Context context,
                                            ITemplateHolder template,
                                            IScanListIterator<IEntryCacheInfo> toScan,
                                            boolean makeWaitForInfo)
            throws TransactionException, TemplateDeletedException,
            SAException {


        long leaseFilter = SystemTime.timeMillis();
        boolean needMatch = !toScan.isAlreadyMatched();
        int alreadyMatchedFixedPropertyIndexPos = toScan.getAlreadyMatchedFixedPropertyIndexPos();
        String alreadyMatchedIndexPath = toScan.getAlreadyMatchedIndexPath();
        try {
            while (toScan != null && toScan.hasNext()) {
                IEntryCacheInfo pEntry = toScan.next();
                if (pEntry == null)
                    continue;
                _spaceEngine.getMatchedEntriesAndOperateSA_Entry(context,
                        template,
                        needMatch, alreadyMatchedFixedPropertyIndexPos, alreadyMatchedIndexPath, leaseFilter,
                        pEntry, makeWaitForInfo);
                if (template.getBatchOperationContext().getNumResults() >= template.getBatchOperationContext().getMaxEntries())
                    return;
                if (context.isFifoGroupScanEncounteredXtnConflict()) {
                    context.setFifoGroupScanEncounteredXtnConflict(false);
                    if (toScan.isIterator())
                        ((IFifoGroupIterator) toScan).nextGroup();
                }
            }//for
        }//try
        finally {
            // scan ended, release resource
            if (toScan != null)
                toScan.releaseScan();
            if (toScan.isIterator()) {//readM/takeM we should order the result by groups
//TBD      			 
//readM/takeM we should order the result by groups      			 

            }
            if (context.isPendingExpiredEntriesExist() && _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates()) {
                try {
                    _spaceEngine.getLeaseManager().forceLeaseReaperCycle(false);
                } catch (InterruptedException e) {
                }
                context.setPendingExpiredEntriesExist(false);
            }

        }

    }

    //an entry has encountered a matching fifo-group template waiting
    //if the class supports fifo-groups, see if the fifo-group index has a matching entry
    void scanNonNotifyTemplate(ITemplateHolder template, IEntryHolder entrySubject,
                               Context context, boolean ifExist)
            throws EntryDeletedException, SAException, TemplateDeletedException {
        final IServerTypeDesc serverTypeDesc = _spaceEngine.getTypeTableEntry(template.getClassName());

        IScanListIterator<IEntryCacheInfo> toScan = _cacheManager.getFifoGroupCacheImpl().getScannableEntriesByIndexValue
                (context, template, entrySubject, serverTypeDesc);
        if (toScan == null)
            return; //no entry or f-g not supported for type

        long leaseFilter = SystemTime.timeMillis();
        boolean needMatch = true;
        int alreadyMatchedFixedPropertyIndexPos = toScan.getAlreadyMatchedFixedPropertyIndexPos();
        String alreadyMatchedIndexPath = toScan.getAlreadyMatchedIndexPath();

        try {
            while (toScan.hasNext()) {
                IEntryCacheInfo pEntry = toScan.next();
                if (pEntry == null)
                    continue;
                IEntryHolder res = _spaceEngine.getMatchedEntryAndOperateSA_Entry(context,
                        template, ifExist,
                        needMatch, alreadyMatchedFixedPropertyIndexPos, alreadyMatchedIndexPath, leaseFilter,
                        false /*useSCN*/, pEntry);
                if (res != null) {//we have a f-g operation done. any use to scan more templates ?
                    if (_spaceEngine.getTemplateScanner().match(context, entrySubject, template))
                        context.setExhaustedFifoGroupsTemplatesSearch(true);
                    return;
                }
                if (context.isFifoGroupScanEncounteredXtnConflict()) {
                    context.setFifoGroupScanEncounteredXtnConflict(false);
                    if (toScan.isIterator())
                        ((IFifoGroupIterator) toScan).nextGroup();
                }
            }
        }//try
        catch (TransactionException e) {
        } finally {
            // scan ended, release resource
            toScan.releaseScan();
        }
    }


    public void handleNeedFgOnlyScanOnXtnEnd(Context context, XtnEntry xtnEntry)
            throws SAException {
        try {
            if ((!_cacheManager.isMemorySpace() && _cacheManager.isEvictableCachePolicy()) || !xtnEntry.getXtnData().anyEntriesForFifoGroupScan())
                return; //not relevant, f-g not supported
            if (_cacheManager.getFifoGroupCacheImpl().getNumOfTemplates() == 0)
                return;

            List<IEntryHolder> entries = xtnEntry.getXtnData().getEntriesForFifoGroupScan();
            if (entries != null) {
                for (IEntryHolder entry : entries) {
                    try {
                        _spaceEngine.getTemplateScanner().scanNonNotifyTemplates(context, entry, null/*txn*/, FifoSearch.NO, FifoGroupsSearch.EXCLUSIVE);
                    } catch (EntryDeletedException ex) {
                    }
                }
            }
        } finally {
            xtnEntry.getXtnData().resetEntriesForFifoGroupsScan();
        }
    }

    /**
     * method is called before xtn status change in order to set the entries for f-g scans .
     */
    void prepareForFifoGroupsAfterXtnScans(Context context, final XtnEntry xtnEntry)
            throws SAException {
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        try {
            entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                    xtnEntry, SelectType.ALL_ENTRIES, false /* returnPEntry*/);
            if (entriesIter != null) {
                final XtnData pXtn = xtnEntry.getXtnData();
                while (true) {
                    IEntryHolder entry = entriesIter.next();
                    if (entry == null)
                        break;
                    if (!_cacheManager.getTypeData(entry.getServerTypeDesc()).hasFifoGroupingIndex())
                        continue;
                    pXtn.addToEntriesForFifoGroupScan(entry);
                }
            } /* if entriesiter != null */
        } /* try */ finally {
            if (entriesIter != null)
                entriesIter.close();
        }
    }


    /**
     * f-g operation about to take place. verify & insert to f-g cache annd xtn info
     */
    void fifoGroupsOnOperationAction(Context context, IEntryHolder entry, ITemplateHolder template, IServerTypeDesc tte)
            throws TransactionConflictException {
        Object val = _cacheManager.getTypeData(tte).getFifoGroupingIndex().getIndexValue(entry.getEntryData());
        if (context.isMultipleOperation() && context.isFifoGroupValueForMiltipleOperations(val))
            return;
        if (!_cacheManager.testAndSetFGCacheForEntry(context, entry, template, template.getXidOriginated() == null/* testOnly*/, tte))
            throw _spaceEngine.TX_CONFLICT_EXCEPTION;
        template.getXidOriginated().getXtnData().addToFifoGroupsEntries(entry, val);
        if (context.isMultipleOperation())
            context.setToFifoGroupValueForMiltipleOperations(val);
    }


    //if entry was deleted maybe fifo-groups templates are waiting for
    //this group to become avail 
    public void handleRemoveWaitingForInfoSAFifoGroups_EntryBased(Context context, IEntryHolder entry,
                                                                  ITemplateHolder template)
            throws SAException {
        if (template != null && template.isFifoGroupPoll() && !template.isDeleted()) {//specific template, handle it and return
            try {
                scanNonNotifyTemplate(template, entry, context, true /*ifExist*/);
            } catch (TemplateDeletedException ex) {
            } catch (EntryDeletedException ex) {
            }
        } else {
            //create WF iterator on clone
            Collection<ITemplateHolder> wfclone = null;
            ILockObject entryLock = _cacheManager.getLockManager().getLockObject(entry);
            try {
                synchronized (entryLock) {
                    wfclone = entry.getCopyOfTemplatesWaitingForEntry();
                }
            } finally {
                _cacheManager.getLockManager().freeLockObject(entryLock);
            }

            if (wfclone == null || wfclone.isEmpty())
                return;

            for (ITemplateHolder th : wfclone) {
                if (th.isFifoGroupPoll() && !th.isDeleted()) {//specific template, handle it and return
                    try {
                        scanNonNotifyTemplate(th, entry, context, true /*ifExist*/);
                    } catch (TemplateDeletedException ex) {
                        continue;
                    } catch (EntryDeletedException ex) {
                    }
                }
            } /* while (true) */
        }
    } /* try */

}
