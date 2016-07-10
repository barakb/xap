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

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateOnceMap;
import com.j_spaces.core.client.Modifiers;

import java.util.Map;

/**
 * Provides modifiers to customize the behavior of take operations.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */

public class TakeModifiers extends SpaceProxyOperationModifiers {
    private static final long serialVersionUID = 1L;

    /**
     * Empty - use operation default behavior.
     */
    public static final TakeModifiers NONE = new TakeModifiers(Modifiers.NONE);

    /**
     * Remove matching entries from memory only (do not remove from the underlying EDS). Ignored if
     * there's no underlying EDS or if the caching policy is ALL_IN_CACHE. If the operation is
     * invoked with a timeout, the timeout is ignored.
     */
    public static final TakeModifiers EVICT_ONLY = new TakeModifiers(Modifiers.EVICT_ONLY);

    /**
     * If one or more partitions are not available during the operation, ignore them and return
     * partial results based on the available partitions (instead of throwing an
     * TakeMultipleException which contains the partial results, which is the default behavior).
     * Relevant only for take multiple operations when the result set maximum size is not reached.
     */
    public static final TakeModifiers IGNORE_PARTIAL_FAILURE = new TakeModifiers(Modifiers.IGNORE_PARTIAL_FAILURE);

    /**
     * Search for matching entries using First-In-First-Out order.
     */
    public static final TakeModifiers FIFO = new TakeModifiers(Modifiers.FIFO);

    /**
     * Search for matching entries in the FIFO group indicated in the template. The template must
     * have a property marked with the {@link SpaceFifoGroupingProperty}.
     *
     * @see {@link SpaceFifoGroupingProperty}
     */
    public static final TakeModifiers FIFO_GROUPING_POLL = new TakeModifiers(Modifiers.FIFO_GROUPING_POLL);

    /**
     * Search for matching entries in cache memory only (do not use the underlying EDS).
     */
    public static final TakeModifiers MEMORY_ONLY_SEARCH = new TakeModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    /**
     * Blocks only if there's a matching entry which is locked by another transaction. If the
     * timeout elapses and the matching entry is still locked, an exception will be thrown.
     *
     * @since 10.0
     */
    public static final TakeModifiers IF_EXISTS = new TakeModifiers(Modifiers.IF_EXISTS);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(EVICT_ONLY.getCode(), EVICT_ONLY);
        initialValues.put(IGNORE_PARTIAL_FAILURE.getCode(), IGNORE_PARTIAL_FAILURE);
        initialValues.put(FIFO.getCode(), FIFO);
        initialValues.put(FIFO_GROUPING_POLL.getCode(), FIFO_GROUPING_POLL);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        initialValues.put(IF_EXISTS.getCode(), IF_EXISTS);
        return initialValues;
    }

    public TakeModifiers() {
    }

    private TakeModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public TakeModifiers(TakeModifiers modifiers1, TakeModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public TakeModifiers(TakeModifiers modifiers1, TakeModifiers modifiers2, TakeModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public TakeModifiers(TakeModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(TakeModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public TakeModifiers add(TakeModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public TakeModifiers remove(TakeModifiers modifiers) {
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
     * Checks if this instance contains the {@link #IGNORE_PARTIAL_FAILURE} setting.
     *
     * @return true if this instance contains the {@link #IGNORE_PARTIAL_FAILURE} setting, false
     * otherwise.
     */
    public boolean isIgnorePartialFailure() {
        return contains(IGNORE_PARTIAL_FAILURE);
    }

    /**
     * Checks if this instance contains the {@link #FIFO} setting.
     *
     * @return true if this instance contains the {@link #FIFO} setting, false otherwise.
     */
    public boolean isFifo() {
        return contains(FIFO);
    }

    /**
     * Checks if this instance contains the {@link #FIFO_GROUPING_POLL} setting.
     *
     * @return true if this instance contains the {@link #FIFO_GROUPING_POLL} setting, false
     * otherwise.
     */
    public boolean isFifoGroupingPoll() {
        return contains(FIFO_GROUPING_POLL);
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

    /**
     * Checks if this instance contains the {@link #IF_EXISTS} setting.
     *
     * @return true if this instance contains the {@link #IF_EXISTS} setting, false otherwise.
     */
    public boolean isIfExists() {
        return contains(IF_EXISTS);
    }

    @Override
    protected TakeModifiers create(int modifiers) {
        return new TakeModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }

}
