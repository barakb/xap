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

import com.gigaspaces.internal.collections.ShortObjectIterator;
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardShortObjectMap<V> implements ShortObjectMap<V> {
    private final HashMap<Short, V> map;

    public StandardShortObjectMap() {
        map = new HashMap<Short, V>();
    }

    public StandardShortObjectMap(int initialCapacity) {
        map = new HashMap<Short, V>(initialCapacity);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean containsKey(short key) {
        return map.containsKey(key);
    }

    @Override
    public V get(short key) {
        return map.get(key);
    }

    @Override
    public V put(short key, V value) {
        return map.put(key, value);
    }

    @Override
    public short[] keys() {
        short[] keys = new short[map.size()];
        int counter = 0;
        for (Short key : map.keySet())
            keys[counter++] = key;
        return keys;
    }

    @Override
    public ShortObjectIterator<V> iterator() {
        return new StandardShortObjectIterator();
    }

    private class StandardShortObjectIterator implements ShortObjectIterator<V> {

        private final Iterator<Map.Entry<Short, V>> iterator = map.entrySet().iterator();
        private Map.Entry<Short, V> entry;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public void advance() {
            entry = iterator.next();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public short key() {
            return entry.getKey();
        }

        @Override
        public V value() {
            return entry.getValue();
        }
    }
}
