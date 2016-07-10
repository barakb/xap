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

package org.openspaces.core.config;

import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;

import org.openspaces.core.space.BlobStoreDataCachePolicy;
import org.openspaces.core.space.CachePolicy;

/**
 * A factory for creating {@link org.openspaces.core.space.BlobStoreDataCachePolicy} instance.
 *
 * @author Kobi
 * @since 10.0.0, 10.2.0
 */
public class BlobStoreDataPolicyFactoryBean {

    private Integer avgObjectSizeKB;
    private Integer avgObjectSizeBytes;
    private Integer cacheEntriesPercentage;
    private Boolean persistent;

    private BlobStoreStorageHandler blobStoreHandler;

    public void setAvgObjectSizeKB(Integer avgObjectSizeKB) {
        this.avgObjectSizeKB = avgObjectSizeKB;
    }

    public void setAvgObjectSizeBytes(Integer avgObjectSizeBytes) {
        this.avgObjectSizeBytes = avgObjectSizeBytes;
    }

    public Integer getCacheEntriesPercentage() {
        return cacheEntriesPercentage;
    }

    public void setCacheEntriesPercentage(Integer cacheEntriesPercentage) {
        this.cacheEntriesPercentage = cacheEntriesPercentage;
    }

    public Boolean getPersistent() {
        return persistent;
    }

    public void setPersistent(Boolean persistent) {
        this.persistent = persistent;
    }

    public BlobStoreStorageHandler getBlobStoreHandler() {
        return blobStoreHandler;
    }

    public void setBlobStoreHandler(BlobStoreStorageHandler blobStoreHandler) {
        this.blobStoreHandler = blobStoreHandler;
    }

    public CachePolicy asCachePolicy() {
        final BlobStoreDataCachePolicy policy = new BlobStoreDataCachePolicy();
        if (avgObjectSizeKB != null && avgObjectSizeBytes != null) {
            throw new BlobStoreException("avgObjectSizeKB and avgObjectSizeBytes cannot be used together");
        }
        if (avgObjectSizeKB != null)
            policy.setAvgObjectSizeKB(avgObjectSizeKB);
        if (avgObjectSizeBytes != null)
            policy.setAvgObjectSizeBytes(avgObjectSizeBytes);
        if (cacheEntriesPercentage != null)
            policy.setCacheEntriesPercentage(cacheEntriesPercentage);
        if (persistent != null) {
            policy.setPersistent(persistent);
        } else {
            throw new BlobStoreException("persistent attribute in Blobstore space must be configured");
        }
        if (blobStoreHandler != null) {
            policy.setBlobStoreHandler(blobStoreHandler);
        } else {
            throw new BlobStoreException("blobStoreHandler attribute in Blobstore space must be configured");
        }
        return policy;
    }


}
