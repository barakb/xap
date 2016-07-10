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

package com.gigaspaces.internal.io;

import com.gigaspaces.internal.utils.pool.IMemoryAwareResourcePool;

import java.lang.ref.SoftReference;

/**
 * A smart length based caching based on soft reference
 *
 * @author eitany
 * @since 9.6
 */
public abstract class SmartLengthBasedCache<T> {
    private final int _maxCachedBufferLength;
    private final double _expungeRatio;
    private final int _expungeCount;
    private final int _defaultResourceLength;
    private final ISmartLengthBasedCacheCallback _callback;

    private SoftReference<T> _cachedResource;
    private int _usedTooSmallBuffer;

    public SmartLengthBasedCache(int maxCachedBufferLength, double expungeRatio, int expungeCount, int defaultResourceLength, ISmartLengthBasedCacheCallback callback) {
        _maxCachedBufferLength = maxCachedBufferLength;
        _expungeRatio = expungeRatio;
        _expungeCount = expungeCount;
        _defaultResourceLength = defaultResourceLength;
        _callback = callback;
    }

    public T get(int length) {
        T resource = getCachedBuffer();
        if (resource == null || getResourceCapacity(resource) < length) {
            resource = createResource(length);
            tryCacheResource(resource);
        } else if (shouldExpunge(length, resource)) {
            _usedTooSmallBuffer = 0;
            resource = createResource(length);
            tryCacheResource(resource);
        } else {
            prepareResource(resource, length);
        }

        return resource;
    }

    protected void tryCacheResource(T resource) {
        int resourceCapacity = getResourceCapacity(resource);
        if (resourceCapacity > _maxCachedBufferLength)
            return;

        if (_callback != null && !_callback.mayCache(resourceCapacity))
            return;

        _cachedResource = new SoftReference<T>(resource);
    }

    protected abstract void prepareResource(T resource);

    protected abstract void prepareResource(T resource, int length);

    protected abstract T createResource(int length);

    protected abstract int getResourceCapacity(T resource);

    public long getLength() {
        T cachedBuffer = getCachedBuffer();
        if (cachedBuffer == null)
            return 0;
        return getResourceCapacity(cachedBuffer);
    }

    private boolean shouldExpunge(int length, T resource) {
        return length < ((double) getResourceCapacity(resource) * _expungeRatio) && _usedTooSmallBuffer++ >= _expungeCount;
    }

    public T get() {
        T resource = getCachedBuffer();
        if (resource == null) {
            resource = createResource(_defaultResourceLength);
            tryCacheResource(resource);
        } else {
            prepareResource(resource);
        }

        return resource;
    }

    public void set(T resource) {
        if (getResourceCapacity(resource) > _maxCachedBufferLength)
            return;
        tryCacheResource(resource);
        _usedTooSmallBuffer = 0;
    }

    public void notifyUsedSize(int usedSize) {
        T resource = getCachedBuffer();
        if (resource == null)
            return;
        if (shouldExpunge(usedSize, resource))
            _cachedResource = null;
    }

    private T getCachedBuffer() {
        //this is called from ResourcePool concurrently, so needs to be thread safe.
        SoftReference<T> cachedResource = _cachedResource;
        return cachedResource == null ? null : cachedResource.get();
    }

    public static ISmartLengthBasedCacheCallback toCacheCallback(final IMemoryAwareResourcePool resourcePool) {
        return new ISmartLengthBasedCacheCallback() {

            @Override
            public boolean mayCache(long length) {
                return !resourcePool.isLimitReached(length);
            }
        };
    }
}
