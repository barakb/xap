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

import com.gigaspaces.events.NotifyActionType;

import java.util.Map;
import java.util.TreeMap;

/**
 * The Modifier class provides <code>static</code> methods and constants to decode notify types
 * modifiers.  The sets of modifiers are represented as integers with distinct bit positions
 * representing different modifiers.
 *
 * @author Yechiel Fefer
 * @author Igor Goldenberg
 * @version 4.0
 * @deprecated use {@link NotifyActionType}
 */
@Deprecated

public class NotifyModifiers {
    /**
     * The <code>int</code> value representing the <code>Notify None</code> modifier.
     */
    public static final int NOTIFY_NONE = 0x00000000;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_WRITE</code> modifier.
     */
    public static final int NOTIFY_WRITE = 0x00000001;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_UPDATE</code> modifier.
     */
    public static final int NOTIFY_UPDATE = 0x00000002;

    /**
     * The <code>int</code> value representing the <code>Notify Take</code> modifier.
     */
    public static final int NOTIFY_TAKE = 0x00000004;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_LEASE_EXPIRATION</code> modifier.
     */
    public static final int NOTIFY_LEASE_EXPIRATION = 0x00000008;


    /**
     * The <code>int</code> value representing the <code>NOTIFY_ALL</code> modifier which includes
     * the <code>NOTIFY_WRITE</code>, <code>NOTIFY_UPDATE</code>, <code>NOTIFY_TAKE</code> and
     * <code>NOTIFY_LEASE_EXPIRATION</code> modifiers.
     *
     * @deprecated since 9.6 - register using specific modifiers instead.
     */
    @Deprecated
    public static final int NOTIFY_ALL = 0x0000000F;

    /**
     * The <code>int</code> value required matching to be done only by UID if provided (not in POJO)
     * modifier.
     */
    public static final int NOTIFY_MATCH_BY_ID = 0x00000010;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_UNMATCHED</code> modifier.
     */
    public static final int NOTIFY_UNMATCHED = 0x000000020;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_MATCHED_UPDATE</code> modifier.
     *
     * @since 9.1
     */
    public static final int NOTIFY_MATCHED_UPDATE = 0x000000040;

    /**
     * The <code>int</code> value representing the <code>NOTIFY_REMATCHED_UPDATE</code> modifier.
     *
     * @since 9.1
     */
    public static final int NOTIFY_REMATCHED_UPDATE = 0x000000080;


    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NOTIFY_WRITE</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NOTIFY_WRITE</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isWrite(int mod) {
        return (mod & NOTIFY_WRITE) != 0;
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NOTIFY_UPDATE</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NOTIFY_UPDATE</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isUpdate(int mod) {
        return (mod & NOTIFY_UPDATE) != 0;
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NOTIFY_UNMATCHED</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NOTIFY_UNMATCHED</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isUnmatched(int mod) {
        return (mod & NOTIFY_UNMATCHED) != 0;
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NOTIFY_MATCHED_UPDATE</tt>
     * modifier, <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NOTIFY_MATCHED_UPDATE</tt>
     * modifier; <tt>false</tt> otherwise.
     * @since 9.1
     */
    public static boolean isMatchedUpdate(int mod) {
        return (mod & NOTIFY_MATCHED_UPDATE) != 0;
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>NOTIFY_REMATCHED_UPDATE</tt>
     * modifier, <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>NOTIFY_REMATCHED_UPDATE</tt>
     * modifier; <tt>false</tt> otherwise.
     * @since 9.1
     */
    public static boolean isRematchedUpdate(int mod) {
        return (mod & NOTIFY_REMATCHED_UPDATE) != 0;
    }


    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>take</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>take</tt> modifier; <tt>false</tt>
     * otherwise.
     */
    public static boolean isTake(int mod) {
        return (mod & NOTIFY_TAKE) != 0;
    }

    /**
     * Return <tt>true</tt> if the integer argument includes the <tt>lease expiration</tt> modifier,
     * <tt>false</tt> otherwise.
     *
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>lease expiration</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isLeaseExpiration(int mod) {
        return (mod & NOTIFY_LEASE_EXPIRATION) != 0;
    }

    /**
     * @param mod a set of modifiers
     * @return <tt>true</tt> if <code>mod</code> includes the <tt>MATCH_BY_UID</tt> modifier;
     * <tt>false</tt> otherwise.
     */
    public static boolean isMatchByID(int mod) {
        return (mod & NOTIFY_MATCH_BY_ID) != 0;
    }

    private static final Map<Integer, String> _modifiersNames = initModifiersNames();

    private static Map<Integer, String> initModifiersNames() {

        Map<Integer, String> modifiersNames = new TreeMap<Integer, String>();
        modifiersNames.put(NOTIFY_WRITE, "NOTIFY_WRITE");
        modifiersNames.put(NOTIFY_UPDATE, "NOTIFY_UPDATE");
        modifiersNames.put(NOTIFY_TAKE, "NOTIFY_TAKE");
        modifiersNames.put(NOTIFY_LEASE_EXPIRATION, "NOTIFY_LEASE_EXPIRATION");
        modifiersNames.put(NOTIFY_MATCH_BY_ID, "NOTIFY_MATCH_BY_ID");
        modifiersNames.put(NOTIFY_UNMATCHED, "NOTIFY_UNMATCHED");
        modifiersNames.put(NOTIFY_MATCHED_UPDATE, "NOTIFY_MATCHED_UPDATE");
        modifiersNames.put(NOTIFY_REMATCHED_UPDATE, "NOTIFY_REMATCHED_UPDATE");
        return modifiersNames;
    }

    public static String toString(int mod) {
        if (mod == NOTIFY_NONE)
            return "NOTIFY_NONE";

        final StringBuilder sb = new StringBuilder();

        for (Map.Entry<Integer, String> modifier : _modifiersNames.entrySet()) {
            if ((mod & modifier.getKey().intValue()) != 0) {
                if (sb.length() != 0)
                    sb.append('|');
                sb.append(modifier.getValue());
            }
        }

        return sb.toString();
    }
}