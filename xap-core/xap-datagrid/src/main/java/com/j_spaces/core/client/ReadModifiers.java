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
import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.client.TakeMultipleException;

/**
 * The {@link ReadModifiers} class provides <code>static</code> methods and constants to decode Read
 * type modifiers. The sets of modifiers are represented as integers with distinct bit positions
 * representing different modifiers. <p> You could use bitwise or operator "|" to unite different
 * modifiers. <br> <b>Note</b> that {@link #REPEATABLE_READ}, {@link #DIRTY_READ} and {@link
 * #READ_COMMITTED} are mutually exclusive (i.e. can't be used together). While {@link
 * #EXCLUSIVE_READ_LOCK} can be joined with any of them. <p> These modifiers can be set either at
 * the proxy level {@link com.j_spaces.core.IJSpace#setReadModifiers(int)} or at the operation level
 * (e.g. using one of {@link com.j_spaces.core.IJSpace} read/readIfExists/readMultiple/count methods
 * with a modifiers parameter ) <p> The <i>default</i> behavior is that of the {@link
 * #REPEATABLE_READ} modifier (as defined by the JavaSpace specification ).
 *
 * @author Guy Korland
 * @version 1.0
 * @see com.j_spaces.core.IJSpace#setReadModifiers(int)
 * @since 6.0
 */

public class ReadModifiers {
    /**
     * This class should not be instantiated.
     */
    private ReadModifiers() {
    }

    /**
     * Allows read operations to have visibility of entities that are not write-locked or {@link
     * #EXCLUSIVE_READ_LOCK exclusively-locked} by active transactions.<br> This is the default read
     * isolation-level.
     */
    public static final int REPEATABLE_READ = Modifiers.NONE;

    /**
     * Allows non-transactional read operations to have full visibility of the entities in the
     * space, including entities that are {@link #EXCLUSIVE_READ_LOCK exclusively-locked}.<br>
     */
    public static final int DIRTY_READ = Modifiers.DIRTY_READ;

    /**
     * Allows read operations to have exclusive visibility of entities that are not locked by active
     * transactions. Once exclusively locked, no other transactional operation is allowed. This
     * supports the ability to perform reads in a "SELECT FOR UPDATE" fashion.
     */
    public static final int EXCLUSIVE_READ_LOCK = Modifiers.EXCLUSIVE_READ_LOCK;

    /**
     * Allows read operations to have visibility of already committed entities, regardless of the
     * fact that these entities might be updated (with a newer version) or taken under an
     * uncommitted transaction.<br>
     */
    public static final int READ_COMMITTED = Modifiers.READ_COMMITTED;

    /**
     * The <code>int</code> value required matching to be done only by UID if provided (not in POJO)
     * modifier.
     *
     * @since 6.5
     * @deprecated use {@link org.openspaces.core.GigaSpace#takeById(Class, Object, Object)} instead
     */
    @Deprecated
    public static final int MATCH_BY_ID = Modifiers.MATCH_BY_ID;

    /**
     * A modifier passed to read multiple and take multiple operations. The modifier will cause
     * operation specific exception {@link ReadMultipleException},{@link TakeMultipleException}  to
     * be thrown when not all of the requested number of entries are returned and one or more
     * cluster members are not available.
     *
     * @deprecated This is the default behavior, for old behavior use {@link
     * #IGNORE_PARTIAL_FAILURE}
     */
    @Deprecated
    public static final int THROW_PARTIAL_FAILURE = Modifiers.THROW_PARTIAL_FAILURE;

    /**
     * A modifier passed to read multiple and take multiple operations. The modifier will cause
     * partial results to be returned instead of throwing operation specific exception {@link
     * ReadMultipleException},{@link TakeMultipleException} when not all of the requested number of
     * entries are returned and one or more cluster members are not available.
     *
     * @since 7.1
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

    private static final int[] isolationLevelModifiers = new int[]{REPEATABLE_READ, READ_COMMITTED, DIRTY_READ, EXCLUSIVE_READ_LOCK};

    public static int setIsolationLevelModifier(int modifiers, int newIsolationModifier) {
        boolean isIsolationModifier = false;

        for (int modifier : isolationLevelModifiers) {
            if (modifier == newIsolationModifier) {
                isIsolationModifier = true;
                modifiers = Modifiers.add(modifiers, modifier);
            } else {
                modifiers = Modifiers.remove(modifiers, modifier);
            }
        }

        if (!isIsolationModifier)
            throw new IllegalArgumentException("Given modifier [" + newIsolationModifier + "] is not an isolation level modifier.");

        return modifiers;
    }

    /**
     * Checks if the DIRTY_READ bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <code>true</code> if <code>mod</code> includes the {@link #DIRTY_READ} bit is set.
     */
    public static boolean isDirtyRead(int mod) {
        return Modifiers.contains(mod, DIRTY_READ);
    }

    /**
     * Checks if the EXCLUSIVE_READ_LOCK bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <code>true</code> if <code>mod</code> includes the {@link #EXCLUSIVE_READ_LOCK} bit
     * is set.
     */
    public static boolean isExclusiveReadLock(int mod) {
        return Modifiers.contains(mod, EXCLUSIVE_READ_LOCK);
    }

    /**
     * Checks if the READ_COMMITTED bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <code>true</code> if <code>mod</code> includes the {@link #READ_COMMITTED} bit is
     * set.
     */
    public static boolean isReadCommitted(int mod) {
        return Modifiers.contains(mod, READ_COMMITTED);
    }

    /**
     * Checks if the MATCH_BY_ID bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>MATCH_BY_UID</tt> modifier;
     * <tt>false</tt> otherwise.
     * @deprecated use {@link org.openspaces.core.GigaSpace#takeById(Class, Object, Object)} instead
     */
    @Deprecated
    public static boolean isMatchByID(int mod) {
        return Modifiers.contains(mod, MATCH_BY_ID);
    }

    /**
     * Checks if the THROW_PARTIAL_FAILURE bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @deprecated This is the default behavior, for old behavior use {@link
     * #IGNORE_PARTIAL_FAILURE}
     */
    @Deprecated
    public static boolean isThrowPartialFailure(int mod) {
        return !Modifiers.contains(mod, IGNORE_PARTIAL_FAILURE);
    }

    /**
     * Checks if the FIFO bit was set for this modifier.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>FIFO</tt> modifier; <tt>false</tt>
     * otherwise.
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
