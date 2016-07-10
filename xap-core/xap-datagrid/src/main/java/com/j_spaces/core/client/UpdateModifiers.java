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


/**
 * The Modifier class provides <code>static</code> methods and constants to decode update types
 * modifiers.  The sets of modifiers are represented as integers with distinct bit positions
 * representing different modifiers.
 *
 * @author Yechiel Fefer
 * @author Igor Goldenberg
 * @version 4.0
 */

public class UpdateModifiers {
    /**
     * The <code>int</code> value representing the <code>WRITE_ONLY</code> modifier. When set: the
     * entry to be written must not reside in the space, no new entry will be written otherwise and
     * an {@link EntryAlreadyInSpaceException EntryAlreadyInSpaceException} will be thrown.
     *
     * Notice: can't be used in together with {@link #UPDATE_OR_WRITE} or {@link #UPDATE_ONLY}.
     */
    public static final int WRITE_ONLY = Modifiers.WRITE;

    /**
     * The <code>int</code> value representing the <code>UPDATE_OR_ONLY</code> modifier. When set
     * alone: the entry to be updated must be reside in the space, no new entry will be written
     * otherwise and an {@link com.j_spaces.core.client.EntryNotInSpaceException
     * EntryNotInSpaceException} will be thrown.
     */
    public static final int UPDATE_ONLY = Modifiers.UPDATE;

    /**
     * The <code>int</code> value representing the <code>UPDATE_OR_WRITE</code> modifier. When set:
     * if the entry to be updated does not reside in the space, a new entry is written <p> Notice:
     * can't be used in together with {@link #PARTIAL_UPDATE}
     */
    public static final int UPDATE_OR_WRITE = Modifiers.UPDATE_OR_WRITE;

    /**
     * The <code>int</code> value representing the <code>PARTIAL_UPDATE</code> modifier. If set,
     * null values in the updated entry are treated as "leave as it is" <p> Notice: can't be used in
     * together with {@link #UPDATE_OR_WRITE} or {@link #WRITE_ONLY}.
     */
    public static final int PARTIAL_UPDATE = Modifiers.PARTIAL_UPDATE;

    /**
     * If set, the write or update operations will return <code>null</code> instead of the lease
     * object (write) or previous value (update). Using this option will improve application write
     * operation performance - null return value yields less network traffic and less memory
     * consumption. Note: This modifier does not replace ONE_WAY modifier
     **/
    public static final int NO_RETURN_VALUE = Modifiers.NO_RETURN_VALUE;

    /**
     * Determines if an update operation should return the previous entry in the lease.
     *
     * @since 9.0.1
     */
    public static final int RETURN_PREV_ON_UPDATE = Modifiers.RETURN_PREV_ON_UPDATE;

    /**
     * Look only in memory for existence of entry with the same ID -do not use the underlying EDS.
     * We assume that if an entry with the same ID does not reside in cache- it does exist in the
     * underlying EDS either. It is the responsibility of the caller using this modifier to make
     * sure the entry does not reside in the EDS - failing to do so may cause inconsistent behaviour
     * of the system
     *
     * @since 9.1.1
     */
    public static final int MEMORY_ONLY_SEARCH = Modifiers.MEMORY_ONLY_SEARCH;


    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>UPDATE_OR_WRITE</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>public</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isUpdateOrWrite(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.UPDATE_OR_WRITE);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>PARTIAL_UPDATE</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>PARTIAL_UPDATE</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isPartialUpdate(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.PARTIAL_UPDATE);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>UPDATE_ONLY</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>UPDATE_ONLY</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isUpdateOnly(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.UPDATE_ONLY);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>UPDATE_ONLY</tt> modifier or
     * <tt>PARTIAL_UPDATE</tt> modifier  or <tt>UPDATE_OR_WRITE</tt> modifier, <tt>false</tt>
     * otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>UPDATE_ONLY</tt> modifier or
     * <tt>PARTIAL_UPDATE</tt> modifier  or <tt>UPDATE_OR_WRITE</tt> modifier; <tt>false</tt>
     * otherwise.
     */
    public static boolean isUpdate(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.UPDATE_ONLY | UpdateModifiers.PARTIAL_UPDATE | UpdateModifiers.UPDATE_OR_WRITE);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>WRITE_ONLY</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>WRITE_ONLY</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isWriteOnly(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.WRITE_ONLY);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NO_RETURN_VALUE</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NO_RETURN_VALUE</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isNoReturnValue(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.NO_RETURN_VALUE);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>RETURN_PREV_ON_UPDATE</tt>
     * modifier, <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>RETURN_PREV_ON_UPDATE</tt>
     * modifier; <tt>false</tt> otherwise.
     */
    public static boolean isReturnPrevOnUpdate(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.RETURN_PREV_ON_UPDATE);
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>MEMORY_ONLY_SEARCH</tt>
     * modifier, <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>MEMORY_ONLY_SEARCH</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isMemoryOnlySearch(int mod) {
        return Modifiers.contains(mod, UpdateModifiers.MEMORY_ONLY_SEARCH);
    }

    public static boolean isPotentialUpdate(int mod) {
        return isUpdateOrWrite(mod) || isUpdateOnly(mod) || isPartialUpdate(mod);
    }
}
