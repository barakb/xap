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

import com.gigaspaces.internal.collections.ObjectShortMap;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardObjectShortMap<K> implements ObjectShortMap<K> {
    private final Map<K, Short> map;

    public StandardObjectShortMap() {
        this.map = new HashMap<K, Short>();
    }

    public StandardObjectShortMap(Map<K, Short> map) {
        this.map = map;
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public short get(K key) {
        return map.get(key);
    }

    @Override
    public void put(K key, short value) {
        map.put(key, value);
    }

    @Override
    public void serialize(ObjectOutput out) throws IOException {
        out.writeObject(map);
    }
}
