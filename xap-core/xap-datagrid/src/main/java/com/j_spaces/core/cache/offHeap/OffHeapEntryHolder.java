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

//
package com.j_spaces.core.cache.offHeap;

/**
 * extention of entry-holder for off-heap
 *
 * @author yechiel
 * @since 9.8
 */

import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EntryHolderEmbeddedSyncOpInfo;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.EntryHolder;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.offHeap.storage.bulks.BlobStoreBulkInfo;
import com.j_spaces.kernel.locks.ILockObject;

@com.gigaspaces.api.InternalApi
public class OffHeapEntryHolder extends EntryHolder implements IOffHeapEntryHolder {

    /**
     * for off heap only- contains the resident part that is always hooked
     **/
    private OffHeapRefEntryCacheInfo _offHeapResidentPart;

    private short _offHeapVersion;

    private String _typeName;
    private byte _entryTypeCode;

    //not null if entry is part of non-transactional bulk operation
    private BlobStoreBulkInfo _bulkInfo;
    private EntryHolderEmbeddedSyncOpInfo _embeddedSyncOpInfo;

    public OffHeapEntryHolder(IServerTypeDesc typeDesc, String uid, long scn,
                              boolean isTransient, ITransactionalEntryData entryData) {
        super(typeDesc, uid, scn,
                isTransient, entryData);
        _typeName = typeDesc.getTypeName();
        _entryTypeCode = entryData.getEntryTypeDesc().getEntryType().getTypeCode();
        ;
    }

    public OffHeapEntryHolder(IEntryHolder other) {
        super(other);
        _typeName = other.getServerTypeDesc().getTypeDesc().getTypeName();
        _entryTypeCode = getEntryData().getEntryTypeDesc().getEntryType().getTypeCode();
        ;
    }

    //+++++++++++++ ILockObject methods
    @Override
    public ILockObject getExternalLockObject() {
        return _offHeapResidentPart;
    }

    //++++++++++++++ IOffHeapEntryHolder ++++++++++++++
    @Override
    public OffHeapRefEntryCacheInfo getOffHeapResidentPart() {
        if (_offHeapResidentPart == null)
            throw new RuntimeException("external lock object is null for off-heap entry !!!!");
        return _offHeapResidentPart;
    }


    @Override
    public void setOffHeapResidentPart(OffHeapRefEntryCacheInfo offHeapResidentPart) {
        _offHeapResidentPart = offHeapResidentPart;
    }

    @Override
    public boolean isSameEntryInstance(IEntryHolder other) {
        if (this == other)
            return true;
        if (other == null || !other.isOffHeapEntry())
            return false;
        return
                _offHeapResidentPart == ((IOffHeapEntryHolder) other).getOffHeapResidentPart();

    }


    @Override
    public boolean isOffHeapEntry() {
        return true;
    }


    @Override
    public boolean isDeleted() {
        return _offHeapResidentPart.isDeleted();
    }

    @Override
    public void setDeleted(boolean val) {
        super.setDeleted(val);
        _offHeapResidentPart.setDeleted(val);
    }


    @Override
    public IEntryHolder getLatestEntryVersion(CacheManager cacheManager, boolean attatchToMemory, Context attachingContext) {
        return _offHeapResidentPart.getLatestEntryVersion(cacheManager, attatchToMemory, this, attachingContext);
    }

    @Override
    public short getOffHeapVersion() {
        return _offHeapVersion;
    }

    @Override
    public void setOffHeapVersion(short version) {
        _offHeapVersion = version;
    }

    @Override
    public void setDirty(CacheManager cacheManager) {
        _typeName = getTypeName();
        _entryTypeCode = getEntryData().getEntryTypeDesc().getEntryType().getTypeCode();
        ;
        _offHeapResidentPart.setDirty(true, cacheManager);
    }


    @Override
    public String getTypeName() {
        return _typeName;
    }

    @Override
    public byte getEntryTypeCode() {
        return _entryTypeCode;
    }


    @Override
    public void insertOrTouchInternalCache(CacheManager cacheManager) {
        _offHeapResidentPart.insertOrTouchInternalCache(cacheManager, this);
    }

    @Override
    public BlobStoreBulkInfo getBulkInfo() {
        return _bulkInfo;
    }

    @Override
    public void setBulkInfo(BlobStoreBulkInfo bulkInfo) {
        _bulkInfo = bulkInfo;
    }

    //-------------------- embedded sync list related ------------------------------
    @Override
    public EntryHolderEmbeddedSyncOpInfo getEmbeddedSyncOpInfo() {
        return _embeddedSyncOpInfo;
    }

    @Override
    public void setEmbeddedSyncOpInfo(long generationId, long sequenceId, boolean phantom, boolean partOfMultipleUidsInfo) {
        _embeddedSyncOpInfo = new EntryHolderEmbeddedSyncOpInfo(generationId, sequenceId, phantom, partOfMultipleUidsInfo);
    }

    @Override
    public boolean isPhantom() {
        return _embeddedSyncOpInfo != null && _embeddedSyncOpInfo.isPhantom();
    }

}
