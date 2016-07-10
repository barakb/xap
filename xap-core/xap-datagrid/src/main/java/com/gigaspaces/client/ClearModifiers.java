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


package com.gigaspaces.client;

import com.gigaspaces.internal.utils.collections.CopyOnUpdateOnceMap;
import com.j_spaces.core.client.Modifiers;

import java.util.Map;

/**
 * Provides modifiers to customize the behavior of clear operations.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */

public class ClearModifiers extends SpaceProxyOperationModifiers {
    private static final long serialVersionUID = 1L;

    /**
     * Empty - use operation default behavior.
     */
    public static final ClearModifiers NONE = new ClearModifiers(Modifiers.NONE);

    /**
     * Remove matching entries from memory only (do not remove from the underlying EDS). Ignored if
     * there's no underlying EDS or if the caching policy is ALL_IN_CACHE.
     */
    public static final ClearModifiers EVICT_ONLY = new ClearModifiers(Modifiers.EVICT_ONLY);

    /**
     * Search for matching entries in cache memory only (do not use the underlying EDS).
     */
    public static final ClearModifiers MEMORY_ONLY_SEARCH = new ClearModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(EVICT_ONLY.getCode(), EVICT_ONLY);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        return initialValues;
    }


    public ClearModifiers() {
    }

    private ClearModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ClearModifiers(ClearModifiers modifiers1, ClearModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ClearModifiers(ClearModifiers modifiers1, ClearModifiers modifiers2, ClearModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ClearModifiers(ClearModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(ClearModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public ClearModifiers add(ClearModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public ClearModifiers remove(ClearModifiers modifiers) {
        return createIfNeeded(super.remove(modifiers));
    }

    /**
     * Checks if this instance contains the {@link #EVICT_ONLY} setting.
     *
     * @return true if this instance contains the {@link #EVICT_ONLY} setting, false otherwise.
     */
    public boolean isEvictOnly() {
        return contains(EVICT_ONLY);
    }

    /**
     * Checks if this instance contains the {@link #MEMORY_ONLY_SEARCH} setting.
     *
     * @return true if this instance contains the {@link #MEMORY_ONLY_SEARCH} setting, false
     * otherwise.
     */
    public boolean isMemoryOnlySearch() {
        return contains(MEMORY_ONLY_SEARCH);
    }

    @Override
    protected ClearModifiers create(int modifiers) {
        return new ClearModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }

}
