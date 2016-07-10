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


package com.j_spaces.core.client;

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.client.TakeMultipleException;

/**
 * The {@link TakeModifiers} class provides <code>static</code> methods and constants to decode Take
 * type modifiers. The sets of modifiers are represented as integers with distinct bit positions
 * representing different modifiers. <p> These modifiers can be set only at the operation level
 * (e.g. using one of {@link com.j_spaces.core.IJSpace} take/takeIfExists/takeMultiple/clear methods
 * with a modifiers parameter ) <p>
 *
 * @author Yechiel
 * @since 8.0
 */

public class TakeModifiers {
    /**
     * This class should not be instantiated.
     */
    private TakeModifiers() {
    }

    /**
     * A modifier passed to take/clear operations.<br> The modifier will cause the eviction of
     * entries matching the given template from cache.<br> The entries will not be removed from the
     * persistent layer. <br> Relevant for LRU cache policy only.<br> If a timeout parameter is
     * specified it will be ignored with this modifier.
     */
    public static final int EVICT_ONLY = Modifiers.EVICT_ONLY;

    /**
     * A modifier passed to read multiple and take multiple operations. The modifier will cause
     * partial results to be returned instead of throwing operation specific exception {@link
     * TakeMultipleException} when not all of the requested number of entries are returned and one
     * or more cluster members are not available.
     */
    public static final int IGNORE_PARTIAL_FAILURE = Modifiers.IGNORE_PARTIAL_FAILURE;

    /**
     * Indicates the operation should comply with First-In-First-Out order.
     */
    public static final int FIFO = Modifiers.FIFO;

    /**
     * A modifier passed to take & read operations. The modifier will cause a group fifo poll based
     * on the passed template. The template must have a property marked with the {@link
     * SpaceFifoGroupingProperty}
     *
     * @since 9.0.0
     */
    public static final int FIFO_GROUPING_POLL = Modifiers.FIFO_GROUPING_POLL;

    /**
     * Search for matching entries in cache memory only (do not use the underlying EDS).
     *
     * @since 9.0.1
     */
    public static final int MEMORY_ONLY_SEARCH = Modifiers.MEMORY_ONLY_SEARCH;

    /**
     * Checks if the EVICT_ONLY bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <code>true</code> if <code>mod</code> includes the {@link #EVICT_ONLY} bit is set.
     */
    public static boolean isEvictOnly(int mod) {
        return Modifiers.contains(mod, EVICT_ONLY);
    }

    /**
     * Checks if the IGNORE_PARTIAL_FAILURE bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <code>true</code> if <code>mod</code> includes the {@link #IGNORE_PARTIAL_FAILURE}
     * bit is set.
     */
    public static boolean isIgnorePartialFailure(int mod) {
        return Modifiers.contains(mod, IGNORE_PARTIAL_FAILURE);
    }

    /**
     * Checks if the FIFO bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>MATCH_BY_UID</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isFifo(int mod) {
        return Modifiers.contains(mod, FIFO);
    }

    /**
     * Checks if the FIFO_GROUPING_POLL bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>FIFO_GROUPING_POLL</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isFifoGroupingPoll(int mod) {
        return Modifiers.contains(mod, FIFO_GROUPING_POLL);
    }

    /**
     * Checks if the MEMORY_ONLY_SEARCH bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>MEMORY_ONLY_SEARCH</tt> modifier;
     * <tt>false</tt> otherwise.
     * @since 9.0.1
     */
    public static boolean isMemoryOnlySearch(int mod) {
        return Modifiers.contains(mod, MEMORY_ONLY_SEARCH);
    }
}
