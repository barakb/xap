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
 * Provides modifiers to customize the behavior of change operations.
 *
 * @author Niv Ingberg
 * @since 9.1
 */

public class ChangeModifiers extends SpaceProxyOperationModifiers {
    private static final long serialVersionUID = 1L;

    /**
     * Empty - use operation default behavior.
     */
    public static final ChangeModifiers NONE = new ChangeModifiers(Modifiers.NONE);

    /**
     * Operation is executed in one way mode, meaning no return value will be provided. Using this
     * mode provides no guarantee whether the operation succeeded or not, the only guarantee is that
     * the operation was successfully written to the local network buffer. As a result, using this
     * modifier will cause the operation not to guarantee automatic fail-over if the primary space
     * instance failed, and it cannot be done under a transaction.
     */
    public static final ChangeModifiers ONE_WAY = new ChangeModifiers(Modifiers.ONE_WAY);

    /**
     * Search for matching entries in cache memory only (do not use the underlying external data
     * source). However, any changes done on the matches entries will propagate to the underlying
     * external data source.
     */
    public static final ChangeModifiers MEMORY_ONLY_SEARCH = new ChangeModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    /**
     * Return details results meaning the {@link ChangeResult#getResults()} should contain data,
     * otherwise only the number of changed entries will be returned as a result which can be
     * accessed via the {@link ChangeResult#getNumberOfChangedEntries()}.
     */
    public static final ChangeModifiers RETURN_DETAILED_RESULTS = new ChangeModifiers(Modifiers.RETURN_DETAILED_CHANGE_RESULT);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(ONE_WAY.getCode(), ONE_WAY);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        initialValues.put(RETURN_DETAILED_RESULTS.getCode(), RETURN_DETAILED_RESULTS);
        return initialValues;
    }

    public ChangeModifiers() {
    }

    ChangeModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ChangeModifiers(ChangeModifiers modifiers1, ChangeModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ChangeModifiers(ChangeModifiers modifiers1, ChangeModifiers modifiers2, ChangeModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public ChangeModifiers(ChangeModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(ChangeModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public ChangeModifiers add(ChangeModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public ChangeModifiers remove(ChangeModifiers modifiers) {
        return createIfNeeded(super.remove(modifiers));
    }

    @Override
    protected ChangeModifiers create(int modifiers) {
        return new ChangeModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }

}
