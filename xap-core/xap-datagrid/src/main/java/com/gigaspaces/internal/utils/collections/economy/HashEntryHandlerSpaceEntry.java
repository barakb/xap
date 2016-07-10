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


package com.gigaspaces.internal.utils.collections.economy;

import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.kernel.IStoredList;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class HashEntryHandlerSpaceEntry<K>
        implements IHashEntryHandler<K, IStoredList<IEntryCacheInfo>> {
    private final int _keyIndicator;

    public HashEntryHandlerSpaceEntry(int keyIndicator) {
        _keyIndicator = keyIndicator;
    }

    public int hash(IHashEntry<K, IStoredList<IEntryCacheInfo>> e) {
        if (e.isNativeHashEntry())
            return ((EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>) e).hash;
        return EconomyConcurrentHashMap.hash(e.getHashCode(_keyIndicator));
    }

    public K key(IHashEntry<K, IStoredList<IEntryCacheInfo>> e) {
        return e.getKey(_keyIndicator);
    }

    public IStoredList<IEntryCacheInfo> value(IHashEntry<K, IStoredList<IEntryCacheInfo>> e) {
        if (e.isNativeHashEntry())
            return ((EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>) e).value;
        return (IStoredList<IEntryCacheInfo>) e;

    }

    public IHashEntry<K, IStoredList<IEntryCacheInfo>> next(IHashEntry<K, IStoredList<IEntryCacheInfo>> e) {
        if (e.isNativeHashEntry())
            return ((EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>) e).next;
        return null;

    }

    public IHashEntry<K, IStoredList<IEntryCacheInfo>> createEntry(K key, IStoredList<IEntryCacheInfo> value, IHashEntry<K, IStoredList<IEntryCacheInfo>> next, int hash) {
        return createEntry(key, value, next, hash, false /*unstableKey*/);

    }


    public IHashEntry<K, IStoredList<IEntryCacheInfo>> createEntry(K key, IStoredList<IEntryCacheInfo> value, IHashEntry<K, IStoredList<IEntryCacheInfo>> next, int hash, boolean unstableKey) {
        if (next == null && !unstableKey)
            return (IHashEntry<K, IStoredList<IEntryCacheInfo>>) value;

        return unstableKey ? new EconomyConcurrentHashMap.PinnedHashEntry<K, IStoredList<IEntryCacheInfo>>(key, hash, next, value) :
                new EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>(key, hash, next, value);
    }


    public IHashEntry<K, IStoredList<IEntryCacheInfo>> cloneEntry(IHashEntry<K, IStoredList<IEntryCacheInfo>> e, IHashEntry<K, IStoredList<IEntryCacheInfo>> newNext) {
        return
                cloneEntry(e, newNext, false /* unstableKey*/);
    }


    public IHashEntry<K, IStoredList<IEntryCacheInfo>> cloneEntry(IHashEntry<K, IStoredList<IEntryCacheInfo>> e, IHashEntry<K, IStoredList<IEntryCacheInfo>> newNext, boolean unstableKey) {

        int hash = e.getHashCode(_keyIndicator);
        if (e.isNativeHashEntry())
            unstableKey = unstableKey || ((EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>) e).isPinnedHashEntry();
        else {
            if (newNext != null || unstableKey)
                hash = EconomyConcurrentHashMap.hash(hash);
        }

        if (newNext == null && !unstableKey)
            return (IHashEntry<K, IStoredList<IEntryCacheInfo>>) e.getValue(_keyIndicator);


        return unstableKey ? new EconomyConcurrentHashMap.PinnedHashEntry<K, IStoredList<IEntryCacheInfo>>(e.getKey(_keyIndicator), hash, newNext, e.getValue(_keyIndicator))
                : new EconomyConcurrentHashMap.HashEntry<K, IStoredList<IEntryCacheInfo>>(e.getKey(_keyIndicator), hash, newNext, e.getValue(_keyIndicator));

    }

}
