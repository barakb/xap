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
package com.j_spaces.core.cache.offHeap.storage.bulks;

import com.gigaspaces.server.blobstore.BlobStoreBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.offHeap.OffHeapEntryHolder;
import com.j_spaces.core.cache.offHeap.OffHeapEntryLayout;
import com.j_spaces.core.cache.offHeap.OffHeapRefEntryCacheInfo;
import com.j_spaces.core.cache.offHeap.storage.preFetch.BlobStorePreFetchBatchHandler;
import com.j_spaces.core.cache.offHeap.storage.preFetch.BlobStorePreFetchBatchResult;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * handlels prefetch activity
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreReadBulkHandler {

    private final CacheManager _cacheManager;
    Map<OffHeapRefEntryCacheInfo, OffHeapEntryLayout> _res;
    private final BlobStorePreFetchBatchHandler _request;
    private final Logger _logger;


    public BlobStoreReadBulkHandler(CacheManager cacheManager, BlobStorePreFetchBatchHandler request) {
        _cacheManager = cacheManager;
        _request = request;
        _logger = _cacheManager.getLogger();
    }

    public BlobStorePreFetchBatchResult execute() {//NOTE- the returned may contain only part of the requested in case of deleted meanwhile
        BlobStorePreFetchBatchResult res = new BlobStorePreFetchBatchResult(_request);
        Map<Object, OffHeapRefEntryCacheInfo> uids = new HashMap<Object, OffHeapRefEntryCacheInfo>(_request.getEntries().size());
        try {
            List<BlobStoreBulkOperationRequest> operations = new LinkedList<BlobStoreBulkOperationRequest>();
            for (OffHeapRefEntryCacheInfo e : _request.getEntries()) {
                if (e.isDeleted())
                    continue;   //irrelevant
                OffHeapEntryHolder eh = e.getFromInternalCache(_cacheManager);
                if (eh != null) {//found in internal cache
                    res.add(e, eh);
                    continue;
                }
                if ((uids.put(e.getUID(), e)) == null) ;
                operations.add(new BlobStoreGetBulkOperationRequest(e.getStorageKey(), e.getOffHeapStoragePos()));
            }

            if (operations.isEmpty())
                return res; //nothing to actually fertch
            Throwable t = null;
            List<BlobStoreBulkOperationResult> results = _cacheManager.getBlobStoreStorageHandler().executeBulk(operations, BlobStoreObjectType.DATA, false/*transactional*/);
            //scan and if execption in any result- throw it

            for (BlobStoreBulkOperationResult r : results) {
                if (r.getException() != null) {
                    res.setException(r.getException());
                    return res;
                }
                res.add(uids.get(r.getId()), (OffHeapEntryLayout) r.getData());

            }
        } catch (Throwable t) {
            _logger.severe(getClass().getName() + " blobstore:execute-bulk " + t);
            BlobStoreException ex = (t instanceof BlobStoreException) ? (BlobStoreException) t : (new BlobStoreException(t));
            res.setException(ex);
        }
        return res;

    }

}
