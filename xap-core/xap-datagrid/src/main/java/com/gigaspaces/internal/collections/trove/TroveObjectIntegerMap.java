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

import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.gnu.trove.TObjectIntHashMap;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveObjectIntegerMap<K> implements ObjectIntegerMap<K> {
    private final TObjectIntHashMap map = new TObjectIntHashMap();

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public int get(K key) {
        return map.get(key);
    }

    @Override
    public void put(K key, int value) {
        map.put(key, value);
    }

    @Override
    public void clear() {
        map.clear();
    }
}
