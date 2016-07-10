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

import com.gigaspaces.internal.collections.ShortLongIterator;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardShortLongMap implements ShortLongMap {
    private final Map<Short, Long> map;

    public StandardShortLongMap() {
        this.map = new HashMap<Short, Long>();
    }

    public StandardShortLongMap(Map<Short, Long> map) {
        this.map = map;
    }

    @Override
    public boolean containsKey(short key) {
        return map.containsKey(key);
    }

    @Override
    public long get(short key) {
        return map.get(key);
    }

    @Override
    public long put(short key, long value) {
        return map.put(key, value);
    }

    @Override
    public void serialize(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, map);
    }

    @Override
    public ShortLongIterator iterator() {
        return new StandardShortLongIterator();
    }

    private class StandardShortLongIterator implements ShortLongIterator {

        private final Iterator<Map.Entry<Short, Long>> iterator = map.entrySet().iterator();
        private Map.Entry<Short, Long> entry;

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
        public long value() {
            return entry.getValue();
        }
    }
}
