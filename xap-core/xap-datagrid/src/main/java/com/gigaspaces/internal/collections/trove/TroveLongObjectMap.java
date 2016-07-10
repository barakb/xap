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

import com.gigaspaces.internal.collections.LongObjectIterator;
import com.gigaspaces.internal.collections.LongObjectMap;
import com.gigaspaces.internal.gnu.trove.TLongObjectHashMap;
import com.gigaspaces.internal.gnu.trove.TLongObjectIterator;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveLongObjectMap<V> implements LongObjectMap<V> {
    private final TLongObjectHashMap<V> map = new TLongObjectHashMap<V>();

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
        return map.getValues(array);
    }

    @Override
    public LongObjectIterator<V> iterator() {
        return new TroveLongObjectIterator();
    }

    private class TroveLongObjectIterator implements LongObjectIterator<V> {

        private final TLongObjectIterator<V> iterator = map.iterator();

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
        public long key() {
            return iterator.key();
        }

        @Override
        public V value() {
            return iterator.value();
        }
    }
}
