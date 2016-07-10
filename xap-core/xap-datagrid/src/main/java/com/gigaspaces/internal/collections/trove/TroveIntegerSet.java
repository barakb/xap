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

import com.gigaspaces.internal.collections.IntegerSet;
import com.gigaspaces.internal.gnu.trove.TIntHashSet;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveIntegerSet implements IntegerSet {

    private final TIntHashSet set;

    public TroveIntegerSet() {
        this.set = new TIntHashSet();
    }

    public TroveIntegerSet(int initialCapacity) {
        this.set = new TIntHashSet(initialCapacity);
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
        return set.toArray();
    }
}
