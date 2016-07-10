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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.io.ISmartLengthBasedCacheCallback;
import com.gigaspaces.internal.io.SmartLengthBasedCache;
import com.j_spaces.kernel.SystemProperties;

import java.nio.ByteBuffer;

/**
 * A smart buffer caching based on soft reference
 *
 * @author eitany
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SmartByteBufferCache extends SmartLengthBasedCache<ByteBuffer> {
    private static final int DEFAULT_BUFFER_LENGTH = 32;

    public static SmartByteBufferCache getDefaultSmartByteBufferCache() {
        final int maxCachedBufferLength = Integer.getInteger(SystemProperties.LRMI_MAX_CACHED_BUFFER_SIZE, SystemProperties.LRMI_MAX_CACHED_BUFFER_SIZE_DEFAULT);
        final double expungeRatio = Double.parseDouble(System.getProperty(SystemProperties.LRMI_CACHED_BUFFER_EXPUNGE_RATIO, String.valueOf(SystemProperties.LRMI_CACHED_BUFFER_EXPUNGE_RATIO_DEFAULT)));
        final int expungeCount = Integer.getInteger(SystemProperties.LRMI_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD, SystemProperties.LRMI_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD_DEFAULT);
        return new SmartByteBufferCache(maxCachedBufferLength, expungeRatio, expungeCount, null);
    }

    public SmartByteBufferCache(int maxCachedBufferLength, double expungeRatio, int expungeCount, ISmartLengthBasedCacheCallback callback) {
        super(maxCachedBufferLength, expungeRatio, expungeCount, DEFAULT_BUFFER_LENGTH, callback);
    }

    @Override
    protected ByteBuffer createResource(int length) {
        return ByteBuffer.allocate(length);
    }

    @Override
    protected void prepareResource(ByteBuffer resource) {
        resource.clear();
    }

    @Override
    protected void prepareResource(ByteBuffer resource, int length) {
        resource.clear();
        resource.limit(length);
    }

    @Override
    protected int getResourceCapacity(ByteBuffer resource) {
        return resource.capacity();
    }

}
