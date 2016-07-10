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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.lrmi.classloading.LRMIRemoteClassLoaderIdentifier;

/**
 * A thread local class that keeps the context of the remote class loader
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class RemoteClassLoaderContext {
    private final static ThreadLocal<LRMIRemoteClassLoaderIdentifier> _remoteClassLoaderContext = new ThreadLocal<LRMIRemoteClassLoaderIdentifier>() {
        @Override
        protected LRMIRemoteClassLoaderIdentifier initialValue() {
            return new LRMIRemoteClassLoaderIdentifier(-1, -1);
        }
    };


    public static LRMIRemoteClassLoaderIdentifier get() {
        return _remoteClassLoaderContext.get();
    }

    public static LRMIRemoteClassLoaderIdentifier set(LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier) {
        LRMIRemoteClassLoaderIdentifier previous = _remoteClassLoaderContext.get();
        _remoteClassLoaderContext.set(remoteClassLoaderIdentifier);
        return previous;
    }
}
