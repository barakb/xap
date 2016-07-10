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


package com.gigaspaces.client.protective;

import java.util.HashSet;
import java.util.Set;

/**
 * Protective mode setup class, holds the current protective mode setup
 *
 * @author eitany
 * @since 9.1
 */

public class ProtectiveMode {
    /**
     * System property key to enable/disable protective mode entirely, defaults to true,
     */
    public static final String PROTECTIVE_MODE = "com.gs.protectiveMode";
    /**
     * System property key to enable/disable wrong routing usage when writing/updating an entry in a
     * co-located space. If an entry is written/updated to/in the co-located space without a routing
     * value or with a routing value which does not match the space, an exception will be thrown.
     * This is done in order to avoid user error that later on when a remote proxy will try to read
     * this object he will not locate it because the routing value is wrong.
     */
    public static final String WRONG_ENTRY_ROUTING_USAGE = PROTECTIVE_MODE + ".wrongEntryRoutingUsage";
    public static final String TYPE_WITHOUT_ID = PROTECTIVE_MODE + ".typeWithoutId";
    public static final String PRIMITIVE_WITHOUT_NULL_VALUE = PROTECTIVE_MODE + ".primitiveWithoutNullValue";
    public static final String QUERY_WITHOUT_INDEX = PROTECTIVE_MODE + ".queryWithoutIndex";

    private static final String TRUE = Boolean.TRUE.toString();
    private static final String FALSE = Boolean.FALSE.toString();
    private static final String PROTECT_WRONG_ROUTING_USAGE_DEFAULT = TRUE;
    private static final String TYPE_WITHOUT_ID_DEFAULT = TRUE;
    private static final String PRIMITIVE_WITHOUT_NULL_VALUE_DEFAULT = TRUE;
    private static final String QUERY_WITHOUT_INDEX_DEFAULT = FALSE;

    private static final boolean _enabled = Boolean.parseBoolean(System.getProperty(PROTECTIVE_MODE, TRUE));
    private static final boolean _wrongRoutingUsageProtectionEnabled = Boolean.parseBoolean(System.getProperty(WRONG_ENTRY_ROUTING_USAGE, PROTECT_WRONG_ROUTING_USAGE_DEFAULT));
    private static final boolean _typeWithoutIdProtectionEnabled = Boolean.parseBoolean(System.getProperty(TYPE_WITHOUT_ID, TYPE_WITHOUT_ID_DEFAULT));
    private static final boolean _primitiveWithoutNullValueProtectionEnabled = Boolean.parseBoolean(System.getProperty(PRIMITIVE_WITHOUT_NULL_VALUE, PRIMITIVE_WITHOUT_NULL_VALUE_DEFAULT));
    private static final boolean _queryWithoutIndexProtectionEnabled = Boolean.parseBoolean(System.getProperty(QUERY_WITHOUT_INDEX, QUERY_WITHOUT_INDEX_DEFAULT));

    private static final Set<String> _ignoreRoutingProtectiveModeTypeNames = new HashSet<String>();
    private static final Set<String> _ignoreWithoutIdProtectiveModeTypeNames = new HashSet<String>();

    static {
        // Protective mode for Mule's FIFO wrapper object is disabled.
        _ignoreRoutingProtectiveModeTypeNames.add("org.openspaces.esb.mule.queue.OpenSpacesFifoQueueObject");
        // Protective mode for hashed event driven space remoting entry is disabled.
        _ignoreWithoutIdProtectiveModeTypeNames.add("org.openspaces.remoting.HashedEventDrivenSpaceRemotingEntry");
    }

    public static boolean isEnabled() {
        return _enabled;
    }

    public static boolean isWrongRoutingUsageProtectionEnabled() {
        return isEnabled() && _wrongRoutingUsageProtectionEnabled;
    }

    public static boolean shouldIgnoreWrongRoutingProtectiveMode(String typeName) {
        return _ignoreRoutingProtectiveModeTypeNames.contains(typeName);
    }

    public static boolean isTypeWithoutIdProtectionEnabled() {
        return isEnabled() && _typeWithoutIdProtectionEnabled;
    }

    public static boolean shouldIgnoreTypeWithoutIdProtectiveMode(String typeName) {
        return _ignoreWithoutIdProtectiveModeTypeNames.contains(typeName);
    }

    public static boolean isPrimitiveWithoutNullValueProtectionEnabled() {
        return isEnabled() && _primitiveWithoutNullValueProtectionEnabled;
    }

    public static boolean isQueryWithoutIndexProtectionEnabled() {
        return isEnabled() && _queryWithoutIndexProtectionEnabled;
    }
}
