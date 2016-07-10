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

import com.gigaspaces.internal.collections.IntegerSet;

import java.util.HashSet;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardIntegerSet implements IntegerSet {
    private final HashSet<Integer> set;

    public StandardIntegerSet() {
        this.set = new HashSet<Integer>();
    }

    public StandardIntegerSet(int initialCapacity) {
        this.set = new HashSet<Integer>(initialCapacity);
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean contains(int val) {
        return set.contains(val);
    }

    @Override
    public boolean add(int val) {
        return set.add(val);
    }

    @Override
    public int[] toArray() {
        int[] result = new int[set.size()];
        int counter = 0;
        for (Integer value : set)
            result[counter++] = value;
        return result;
    }
}
