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

package com.gigaspaces.internal.collections.standard;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerList;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.collections.IntegerSet;
import com.gigaspaces.internal.collections.LongObjectMap;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.collections.ObjectLongMap;
import com.gigaspaces.internal.collections.ObjectShortMap;
import com.gigaspaces.internal.collections.ShortList;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardCollectionsFactory extends CollectionsFactory {
    @Override
    public <K, V> Map<K, V> createMap() {
        return new HashMap<K, V>();
    }

    @Override
    public <K, V> Map<K, V> createMap(int initialCapacity) {
        return new HashMap<K, V>(initialCapacity);
    }

    @Override
    public <K, V> Map<K, V> createMap(int initialCapacity, float loadFactor) {
        return new HashMap<K, V>(initialCapacity, loadFactor);
    }

    @Override
    public <K, V> Map<K, V> createMap(Map<K, V> map) {
        return new HashMap<K, V>(map);
    }

    @Override
    public ShortList createShortList() {
        return new StandardShortList();
    }

    @Override
    public IntegerList createIntegerList() {
        return new StandardIntegerList();
    }

    @Override
    public IntegerSet createIntegerSet() {
        return new StandardIntegerSet();
    }

    @Override
    public IntegerSet createIntegerSet(int initialCapacity) {
        return new StandardIntegerSet(initialCapacity);
    }

    @Override
    public ShortLongMap createShortLongMap() {
        return new StandardShortLongMap();
    }

    @Override
    protected ShortLongMap deserializeShortLongMap(Object map) {
        return new StandardShortLongMap((Map<Short, Long>) map);
    }

    @Override
    public ShortObjectMap createShortObjectMap() {
        return new StandardShortObjectMap();
    }

    @Override
    public <V> ShortObjectMap<V> createShortObjectMap(int initialCapacity) {
        return new StandardShortObjectMap(initialCapacity);
    }

    @Override
    public <K> ObjectShortMap<K> createObjectShortMap() {
        return new StandardObjectShortMap<K>();
    }

    @Override
    public <V> LongObjectMap<V> createLongObjectMap() {
        return new StandardLongObjectMap();
    }

    @Override
    public <K> ObjectLongMap<K> createObjectLongMap() {
        return new StandardObjectLongMap<K>();
    }

    @Override
    protected <K> ObjectShortMap<K> deserializeObjectShortMap(Object map) {
        return new StandardObjectShortMap<K>((Map<K, Short>) map);
    }

    @Override
    public <V> IntegerObjectMap<V> createIntegerObjectMap() {
        return new StandardIntegerObjectMap<V>();
    }

    @Override
    public <K> ObjectIntegerMap<K> createObjectIntegerMap() {
        return new StandardObjectIntegerMap<K>();
    }
}
