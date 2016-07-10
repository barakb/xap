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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.offHeap.storage.bulks.BlobStoreBulkInfo;

/**
 * The resident part of entry that resides off-heap (SSD)
 *
 * @author yechiel
 * @since 9.8
 */
public interface IOffHeapRefCacheInfo {
    void setDirty(boolean value, CacheManager cacheManager);

    void removeEntryFromOffHeapStorage(CacheManager cacheManager);

    IEntryHolder getLatestEntryVersion(CacheManager cacheManager, boolean attach, IOffHeapEntryHolder lastKnownEntry, Context attachingContext);

    IEntryHolder getLatestEntryVersion(CacheManager cacheManager, boolean attach, IOffHeapEntryHolder lastKnownEntry, Context attachingContext, boolean onlyIndexesPart);

    void unLoadFullEntryIfPossible(CacheManager cacheManager, Context context);

    void unLoadFullEntryIfPossible(CacheManager cacheManager, Context context, boolean fromInitialLoad);

    void setDeleted(boolean deleted);

    IEntryHolder getEntryHolderIfInMemory();

    void flush(CacheManager cacheManager, Context context);

    boolean isInOffHeapStorage();

    Object getOffHeapStoragePos();

    java.io.Serializable getStorageKey();

    java.io.Serializable getEntryLayout(CacheManager cacheManager);

    void flushedFromBulk(CacheManager cacheManager, Object offHeapPos, boolean removed);

    void setOffHeapPosition(Object pos);

    void bulkRegister(Context context, BlobStoreBulkInfo bulkInfo, int spaceOperation, boolean registerDirectPersistency);

    void bulkUnRegister(CacheManager cm);

    boolean isInBulk();

    boolean isBulkFlushing();

    boolean setBulkFlushing(BlobStoreBulkInfo caller);

    OffHeapEntryHolder getFromInternalCache(CacheManager cacheManager);

    void resetNonTransactionalFailedBlobstoreOpStatus(CacheManager cm);

    void buildCrcForFields();

    void setOffHeapVersion(short offHeapVersion);


    //embedded sync list related------------------------------
    boolean isPhantom();

}
