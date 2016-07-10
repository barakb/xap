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

package com.gigaspaces.internal.utils.collections;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.WeakHashMap;

/**
 * This classes uses {@link java.lang.ref.WeakReference} so that contained objects can be collected
 * by the GC.
 *
 * @author Guy Korland
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class WeakHashSet<E> extends AbstractSet<E> {
    private final transient WeakHashMap<E, Boolean> map;

    public WeakHashSet() {
        map = new WeakHashMap<E, Boolean>();
    }

    public WeakHashSet(Collection<E> c) {
        map = new WeakHashMap<E, Boolean>(Math.max((int) (c.size() / .75f) + 1, 16));
        addAll(c);
    }

    public WeakHashSet(int initialCapacity, float loadFactor) {
        map = new WeakHashMap<E, Boolean>(initialCapacity, loadFactor);
    }

    public WeakHashSet(int initialCapacity) {
        map = new WeakHashMap<E, Boolean>(initialCapacity);
    }

    /**
     * @see java.util.Collection#iterator()
     */
    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    /**
     * @see java.util.Collection#size()
     */
    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * @see java.util.Collection#contains(java.lang.Object)
     */
    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /**
     * @see java.util.Collection#add(java.lang.Object)
     */
    @Override
    public boolean add(E o) {
        return map.put(o, Boolean.TRUE) == null;
    }

    /**
     * @see java.util.Set#remove(java.lang.Object)
     */
    @Override
    public boolean remove(Object o) {
        return map.remove(o) == Boolean.TRUE;
    }

    /**
     * @see java.util.Collection#clear()
     */
    @Override
    public void clear() {
        map.clear();
    }
}
