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
package com.j_spaces.core.cache.offHeap.storage.preFetch;

import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.cache.offHeap.OffHeapEntryHolder;
import com.j_spaces.core.cache.offHeap.OffHeapEntryLayout;
import com.j_spaces.core.cache.offHeap.OffHeapRefEntryCacheInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * handlels prefetch activity
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStorePreFetchBatchResult {

    private final Map<OffHeapRefEntryCacheInfo, OffHeapEntryLayout> _resultFromStore;
    private final Map<OffHeapRefEntryCacheInfo, OffHeapEntryHolder> _resultFromCache;
    private Throwable _exception;
    //private final BlobStorePreFetchBatchHandler _handler; //for debugging

    public BlobStorePreFetchBatchResult(BlobStorePreFetchBatchHandler req) {
        _resultFromStore = new HashMap<OffHeapRefEntryCacheInfo, OffHeapEntryLayout>(req.getEntries().size());
        _resultFromCache = new HashMap<OffHeapRefEntryCacheInfo, OffHeapEntryHolder>();
    }

    public void add(OffHeapRefEntryCacheInfo e, OffHeapEntryLayout entry) {
        _resultFromStore.put(e, entry);
    }

    public void add(OffHeapRefEntryCacheInfo e, OffHeapEntryHolder entry) {
        _resultFromCache.put(e, entry);
    }

    public void setException(Throwable exception) {
        _exception = exception;
    }

    public void throwExceptionIfOccured() {
        if (_exception == null)
            return;
        if (_exception instanceof BlobStoreException)
            throw (BlobStoreException) _exception;
        else
            throw new BlobStoreException(_exception);
    }

    public OffHeapEntryLayout getFromStore(OffHeapRefEntryCacheInfo e) {
        throwExceptionIfOccured();
        return _resultFromStore.get(e);
    }

    public OffHeapEntryHolder getFromCache(OffHeapRefEntryCacheInfo e) {
        throwExceptionIfOccured();
        return _resultFromCache.get(e);
    }


}
