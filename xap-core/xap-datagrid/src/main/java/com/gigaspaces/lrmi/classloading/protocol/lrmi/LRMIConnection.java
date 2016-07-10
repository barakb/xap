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

package com.gigaspaces.lrmi.classloading.protocol.lrmi;

import com.gigaspaces.lrmi.classloading.IClassProvider;
import com.gigaspaces.lrmi.classloading.IRemoteClassProviderProvider;
import com.gigaspaces.lrmi.classloading.LRMIRemoteClassLoaderIdentifier;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;

import java.io.IOException;

/**
 * getches class definitions. the url structure should be: lrmi://com.somepackage.SomeClass
 */
@com.gigaspaces.api.InternalApi
public class LRMIConnection {
    private static final ThreadLocal<IRemoteClassProviderProvider> _connectionHolder = new ThreadLocal<IRemoteClassProviderProvider>();

    public static IRemoteClassProviderProvider putConnection(IRemoteClassProviderProvider conn) {
        IRemoteClassProviderProvider previous = _connectionHolder.get();
        _connectionHolder.set(conn);
        return previous;
    }

    public static void setConnection(IRemoteClassProviderProvider conn) {
        _connectionHolder.set(conn);
    }

    public static void clearConnection() {
        _connectionHolder.set(null);
    }

    public static LRMIRemoteClassLoaderIdentifier getRemoteClassLoaderIdentifier() {
        IRemoteClassProviderProvider connection = _connectionHolder.get();
        if (connection != null)
            return connection.getRemoteClassLoaderIdentifier();
        return null;
    }

    public static LRMIRemoteClassLoaderIdentifier setRemoteClassLoaderIdentifier(LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier) {
        IRemoteClassProviderProvider connection = _connectionHolder.get();
        if (connection != null)
            return connection.setRemoteClassLoaderIdentifier(remoteClassLoaderIdentifier);
        return null;
    }

    public static IClassProvider getClassProvider() throws IOException, IOFilterException {
        IRemoteClassProviderProvider connection = _connectionHolder.get();
        if (connection != null)
            return connection.getClassProvider();

        return null;
    }

    public static IRemoteClassProviderProvider getClassProviderProvider() {
        return _connectionHolder.get();
    }

}
