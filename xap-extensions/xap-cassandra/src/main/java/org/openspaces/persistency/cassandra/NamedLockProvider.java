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

package org.openspaces.persistency.cassandra;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a thread safe name based lock interface for getting a shared lock instance based on its
 * name.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class NamedLockProvider {

    private final ConcurrentMap<String, ReentrantLock> lockCache;

    public NamedLockProvider() {
        lockCache = new MapMaker().makeComputingMap(new Function<String, ReentrantLock>() {
            public ReentrantLock apply(String name) {
                return new ReentrantLock();
            }
        });
    }

    /**
     * @param name The name of the lock to get
     * @return The shared {@link java.util.concurrent.locks.ReentrantLock} matching the provided
     * name. One is created if it doesn't already exist. Note that the returned {@link
     * java.util.concurrent.locks.ReentrantLock} should still be aquired and released by the client
     * calling this method.
     */
    public ReentrantLock forName(String name) {
        return lockCache.get(name);
    }

}
