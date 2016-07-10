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

import com.gigaspaces.internal.collections.LongObjectIterator;
import com.gigaspaces.internal.collections.LongObjectMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardLongObjectMap<V> implements LongObjectMap<V> {
    private final HashMap<Long, V> map = new HashMap<Long, V>();

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public V get(long key) {
        return map.get(key);
    }

    @Override
    public void put(long key, V value) {
        map.put(key, value);
    }

    @Override
    public V remove(long key) {
        return map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public V[] getValues(V[] array) {
        int counter = 0;
        for (V value : map.values()) {
            array[counter++] = value;
        }
        return array;
    }

    @Override
    public LongObjectIterator<V> iterator() {
        return new StandardLongObjectIterator();
    }

    private class StandardLongObjectIterator implements LongObjectIterator<V> {
        private final Iterator<Map.Entry<Long, V>> iterator = map.entrySet().iterator();
        private Map.Entry<Long, V> entry;

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
        public long key() {
            return entry.getKey();
        }

        @Override
        public V value() {
            return entry.getValue();
        }
    }
}
