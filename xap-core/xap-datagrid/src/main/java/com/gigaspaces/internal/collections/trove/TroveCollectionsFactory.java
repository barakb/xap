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

package com.gigaspaces.internal.collections.trove;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerList;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.collections.IntegerSet;
import com.gigaspaces.internal.collections.LongObjectMap;
import com.gigaspaces.internal.collections.MapProcedure;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.collections.ObjectLongMap;
import com.gigaspaces.internal.collections.ObjectShortMap;
import com.gigaspaces.internal.collections.ShortList;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.gnu.trove.THashMap;
import com.gigaspaces.internal.gnu.trove.TObjectObjectProcedure;
import com.gigaspaces.internal.gnu.trove.TObjectShortHashMap;
import com.gigaspaces.internal.gnu.trove.TShortLongHashMap;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveCollectionsFactory extends CollectionsFactory {

    @Override
    public <K, V> Map<K, V> createMap() {
        return new THashMap<K, V>();
    }

    @Override
    public <K, V> Map<K, V> createMap(int initialCapacity) {
        return new THashMap<K, V>(initialCapacity);
    }

    @Override
    public <K, V> Map<K, V> createMap(int initialCapacity, float loadFactor) {
        return new THashMap<K, V>(initialCapacity, loadFactor);
    }

    @Override
    public <K, V> Map<K, V> createMap(Map<K, V> map) {
        return new THashMap<K, V>(map);
    }

    @Override
    public <K, V> boolean forEachEntry(Map<K, V> map, final MapProcedure<K, V> procedure) {
        if (map instanceof THashMap) {
            return ((THashMap<K, V>) map).forEachEntry(new TObjectObjectProcedure<K, V>() {
                @Override
                public boolean execute(K key, V value) {
                    return procedure.execute(key, value);
                }
            });
        }
        return super.forEachEntry(map, procedure);
    }

    @Override
    public ShortList createShortList() {
        return new TroveShortList();
    }

    @Override
    public IntegerList createIntegerList() {
        return new TroveIntegerList();
    }

    @Override
    public IntegerSet createIntegerSet() {
        return new TroveIntegerSet();
    }

    @Override
    public IntegerSet createIntegerSet(int initialCapacity) {
        return new TroveIntegerSet(initialCapacity);
    }

    @Override
    public ShortLongMap createShortLongMap() {
        return new TroveShortLongMap();
    }

    @Override
    protected ShortLongMap deserializeShortLongMap(Object map) {
        return new TroveShortLongMap((TShortLongHashMap) map);
    }

    @Override
    public ShortObjectMap createShortObjectMap() {
        return new TroveShortObjectMap();
    }

    @Override
    public <V> ShortObjectMap<V> createShortObjectMap(int initialCapacity) {
        return new TroveShortObjectMap(initialCapacity);
    }

    @Override
    public <K> ObjectShortMap<K> createObjectShortMap() {
        return new TroveObjectShortMap<K>();
    }

    @Override
    public <V> LongObjectMap<V> createLongObjectMap() {
        return new TroveLongObjectMap<V>();
    }

    @Override
    public <K> ObjectLongMap<K> createObjectLongMap() {
        return new TroveObjectLongMap<K>();
    }

    @Override
    protected <K> ObjectShortMap<K> deserializeObjectShortMap(Object map) {
        return new TroveObjectShortMap<K>((TObjectShortHashMap<K>) map);
    }

    @Override
    public <V> IntegerObjectMap<V> createIntegerObjectMap() {
        return new TroveIntegerObjectMap<V>();
    }

    @Override
    public <K> ObjectIntegerMap<K> createObjectIntegerMap() {
        return new TroveObjectIntegerMap<K>();
    }
}
