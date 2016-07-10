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
 * Provides modifiers to customize the behavior of count operations.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */

public class CountModifiers extends IsolationLevelModifiers {
    private static final long serialVersionUID = -6407978572185535198L;

    /**
     * Empty - use operation default behavior.
     */
    public static final CountModifiers NONE = new CountModifiers(Modifiers.NONE);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is honored and the entry is skipped (if the operation is invoked with a
     * timeout it will block until the entry will be released or the timeout expires). If the
     * operation is invoked in a transactional context, the resulting entries will be locked,
     * blocking update/remove from other transactions.
     */
    public static final CountModifiers REPEATABLE_READ = new CountModifiers(Modifiers.NONE);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is honored and matching is performed on the committed state.
     */
    public static final CountModifiers READ_COMMITTED = new CountModifiers(Modifiers.READ_COMMITTED);

    /**
     * When a search for matching entries encounters an entry locked under a different uncommitted
     * transaction, the lock is ignored and matching is performed on the uncommitted state.
     */
    public static final CountModifiers DIRTY_READ = new CountModifiers(Modifiers.DIRTY_READ);

    /**
     * The resulting entries are locked, blocking update/remove from other transactions.
     */
    public static final CountModifiers EXCLUSIVE_READ_LOCK = new CountModifiers(Modifiers.EXCLUSIVE_READ_LOCK);

    /**
     * Search for matching entries in cache memory only (do not use the underlying EDS).
     */
    public static final CountModifiers MEMORY_ONLY_SEARCH = new CountModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(REPEATABLE_READ.getCode(), REPEATABLE_READ);
        initialValues.put(READ_COMMITTED.getCode(), READ_COMMITTED);
        initialValues.put(DIRTY_READ.getCode(), DIRTY_READ);
        initialValues.put(EXCLUSIVE_READ_LOCK.getCode(), EXCLUSIVE_READ_LOCK);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        return initialValues;
    }

    /**
     * Required for Externalizable
     */
    public CountModifiers() {
    }

    private CountModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public CountModifiers(CountModifiers modifiers1, CountModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public CountModifiers(CountModifiers modifiers1, CountModifiers modifiers2, CountModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public CountModifiers(CountModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(CountModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public CountModifiers add(CountModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public CountModifiers remove(CountModifiers modifiers) {
        return createIfNeeded(super.remove(modifiers));
    }

    /**
     * Creates a new modifiers instance with the specified isolation level set and any other
     * isolation level unset.
     *
     * @param isolationLevel The isolation level to set.
     * @return The modifiers from this instance with the new isolation level set and any previous
     * one removed.
     */
    public CountModifiers setIsolationLevel(CountModifiers isolationLevel) {
        return super.setIsolationLevel(isolationLevel);
    }

    /**
     * Checks if this instance contains the {@link #EXCLUSIVE_READ_LOCK} setting.
     *
     * @return true if this instance contains the {@link #EXCLUSIVE_READ_LOCK} setting, false
     * otherwise.
     */
    public boolean isExclusiveReadLock() {
        return contains(EXCLUSIVE_READ_LOCK);
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
    protected CountModifiers create(int modifiers) {
        return new CountModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }
}
