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

import com.gigaspaces.internal.collections.IntegerObjectMap;

import java.util.HashMap;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardIntegerObjectMap<V> implements IntegerObjectMap<V> {
    private final HashMap<Integer, V> map = new HashMap<Integer, V>();

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public V get(int key) {
        return map.get(key);
    }

    @Override
    public void put(int key, V value) {
        map.put(key, value);
    }

    @Override
    public void remove(int key) {
        map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int[] keys() {
        int[] keys = new int[map.size()];
        int counter = 0;
        for (Integer key : map.keySet())
            keys[counter++] = key;
        return keys;
    }

    @Override
    public V[] getValues(V[] array) {
        int counter = 0;
        for (V value : map.values())
            array[counter++] = value;
        return array;
    }
}
