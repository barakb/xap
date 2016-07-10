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

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.j_spaces.core.cache.fifoGroup.ExtendedCompoundIndexFifoGroupsIterator;
import com.j_spaces.core.cache.fifoGroup.ExtendedIndexFifoGroupsIterator;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Handles extended index of fifo groups scans
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class FifoGroupsExtendedIndexHandler<K>
        implements IExtendedEntriesIndex<K, IEntryCacheInfo> {
    private final IExtendedIndex<K, IEntryCacheInfo> _base;

    private final FastConcurrentSkipListMap<Object, IStoredList<IEntryCacheInfo>> _orderedStore;
    private final TypeDataIndex _index;
    private final RecentExtendedIndexUpdates _recentExtendedIndexUpdates;

    private static final boolean FORCE_ORDERED_SCAN = false;

    public FifoGroupsExtendedIndexHandler(TypeDataIndex index, IExtendedIndex<K, IEntryCacheInfo> base, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        if (fifoGroupsIndexType != ISpaceIndex.FifoGroupsIndexTypes.MAIN && fifoGroupsIndexType != ISpaceIndex.FifoGroupsIndexTypes.COMPOUND)
            throw new UnsupportedOperationException();

        _base = base;
        _orderedStore = ((ExtendedIndexHandler) _base).getOrderedStore();
        _index = index;
        _recentExtendedIndexUpdates = null;

    }

    public IObjectInfo insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, boolean alreadyCloned) {
        throw new UnsupportedOperationException();
    }

    public void removeEntryIndexedField(IEntryHolder eh,
                                        Object fieldValue, IEntryCacheInfo pEntry, IObjectInfo oi) {
        throw new UnsupportedOperationException();
    }

    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? returns an
     * ExtendedIndexIterator object which enables scanning the ordered index, Null if no relevant
     * elements to scan
     */
    public IExtendedIndexIterator<IEntryCacheInfo> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive) {
        return
                establishScan(startPos, relation, endPos, endPosInclusive, false /* ordered*/);


    }

    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? ordered - according to the
     * condition. GT, GE ==> ascending, LT, LE =====> descending. returns an IOrderedIndexScan
     * object which enables scanning the ordered index, Null if no relevant elements to scan
     */
    public IExtendedIndexIterator<IEntryCacheInfo> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive, boolean ordered) {
        ordered |= FORCE_ORDERED_SCAN; //should we force ordered scan always ?

        return ordered ?
                establishScanOrdered(startPos, relation, endPos, endPosInclusive) :
                establishScanUnOrdered(startPos, relation, endPos, endPosInclusive);
    }

    @Override
    public void onUpdate(IEntryCacheInfo eci) {
        if (_recentExtendedIndexUpdates != null)
            _recentExtendedIndexUpdates.onUpdate(eci);
    }

    @Override
    public void onUpdateEnd(IEntryCacheInfo eci) {
        if (_recentExtendedIndexUpdates != null)
            _recentExtendedIndexUpdates.onUpdateEnd(eci);
    }

    @Override
    public void onRemove(IEntryCacheInfo eci) {
        if (_recentExtendedIndexUpdates != null)
            _recentExtendedIndexUpdates.onRemove(eci);
    }

    @Override
    public int reapExpired() {
        if (_recentExtendedIndexUpdates != null)
            return _recentExtendedIndexUpdates.reapExpired();
        return 0;
    }


    private IExtendedIndexIterator<IEntryCacheInfo> establishScanUnOrdered(K startPos, short relation, K endPos, boolean endPosInclusive) {
        boolean reversedScan = (relation == TemplateMatchCodes.LT || relation == TemplateMatchCodes.LE);

        K start;
        K end;
        boolean endInclusive;
        boolean startinclusive;
        if (reversedScan) {
            start = endPos;
            startinclusive = endPosInclusive;
            end = startPos;
            endInclusive = relation == TemplateMatchCodes.LE;
        } else {
            startinclusive = relation == TemplateMatchCodes.GE;
            start = startPos;
            end = endPos;
            endInclusive = endPosInclusive;

        }

        NavigableMap baseMap = _orderedStore;
        NavigableMap mapToScan;
        if (end == null)
            mapToScan = start != null ? baseMap.tailMap(start, startinclusive) : baseMap;
        else
            mapToScan = start != null ? baseMap.subMap(start, startinclusive, end, endInclusive) : baseMap.headMap(end, endInclusive);

        if (_index.isCompound()) {
            SimpleCompoundIndexValueHolder s = start != null ? (SimpleCompoundIndexValueHolder) start : (SimpleCompoundIndexValueHolder) end;
            Object nonFGVal = s.getValueBySegment(1);
            return new ExtendedCompoundIndexFifoGroupsIterator<IEntryCacheInfo>(mapToScan, _index, nonFGVal);
        } else
            return new ExtendedIndexFifoGroupsIterator<IEntryCacheInfo>(mapToScan, _index);
    }

    private IExtendedIndexIterator<IEntryCacheInfo> establishScanOrdered(K startPos, short relation, K endPos, boolean endPosInclusive) {
        boolean reversedScan = (relation == TemplateMatchCodes.LT || relation == TemplateMatchCodes.LE);
        boolean startinclusive = (relation == TemplateMatchCodes.GE || relation == TemplateMatchCodes.LE);

        NavigableMap baseMap = reversedScan ? _orderedStore.descendingMap() : _orderedStore;
        NavigableMap mapToScan;
        if (endPos == null)
            mapToScan = startPos != null ? baseMap.tailMap(startPos, startinclusive) : baseMap;
        else
            mapToScan = startPos != null ? baseMap.subMap(startPos, startinclusive, endPos, endPosInclusive) : baseMap.headMap(endPos, endPosInclusive);
        return new ExtendedIndexFifoGroupsIterator<IEntryCacheInfo>(mapToScan, _index);
    }


    @Override
    public ConcurrentMap<Object, IStoredList<IEntryCacheInfo>> getNonUniqueEntriesStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<Object, IEntryCacheInfo> getUniqueEntriesStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FastConcurrentSkipListMap<Object, IStoredList<IEntryCacheInfo>> getOrderedStore() {
        throw new UnsupportedOperationException();
    }


    @Override
    public IStoredList<IEntryCacheInfo> getIndexEntries(K indexValue) {
        return _orderedStore.get(indexValue);
    }


}