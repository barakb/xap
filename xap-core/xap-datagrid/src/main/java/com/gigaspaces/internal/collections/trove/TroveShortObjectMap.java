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

import com.gigaspaces.internal.collections.ShortObjectIterator;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.gnu.trove.TShortObjectHashMap;
import com.gigaspaces.internal.gnu.trove.TShortObjectIterator;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveShortObjectMap<V> implements ShortObjectMap<V> {
    private final TShortObjectHashMap<V> map;

    public TroveShortObjectMap() {
        map = new TShortObjectHashMap<V>();
    }

    public TroveShortObjectMap(int initialCapacity) {
        map = new TShortObjectHashMap<V>(initialCapacity);
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
        return map.keys();
    }

    @Override
    public ShortObjectIterator<V> iterator() {
        return new TroveShortObjectIterator();
    }

    private class TroveShortObjectIterator implements ShortObjectIterator<V> {

        private final TShortObjectIterator<V> iterator = map.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public void advance() {
            iterator.advance();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public short key() {
            return iterator.key();
        }

        @Override
        public V value() {
            return iterator.value();
        }
    }
}
