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

import com.gigaspaces.internal.collections.ObjectShortMap;
import com.gigaspaces.internal.gnu.trove.TObjectShortHashMap;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveObjectShortMap<K> implements ObjectShortMap<K> {
    private final TObjectShortHashMap<K> map;

    public TroveObjectShortMap() {
        map = new TObjectShortHashMap<K>();
    }

    public TroveObjectShortMap(TObjectShortHashMap<K> map) {
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
