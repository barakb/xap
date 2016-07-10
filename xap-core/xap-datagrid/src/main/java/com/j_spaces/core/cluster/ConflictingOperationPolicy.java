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

package com.j_spaces.core.cluster;

/**
 * Defines which policy to use when replication conflicts with the target data. For example: write
 * operation is received an the object already exists at the target.
 *
 * @author anna
 * @since 8.0
 */
public enum ConflictingOperationPolicy {

    /**
     * Override the data on target and continue replication
     */
    OVERRIDE,
    /**
     * Ignore the replicated operation and continue replication
     */
    IGNORE;

    public final static ConflictingOperationPolicy DEFAULT = IGNORE;

    public final static String OVERRIDE_MODE = "override";
    public final static String IGNORE_MODE = "ignore";


    public static ConflictingOperationPolicy parseConflictingPacketsPolicy(
            String value) {
        if (OVERRIDE_MODE.equalsIgnoreCase(value))
            return OVERRIDE;
        if (IGNORE_MODE.equalsIgnoreCase(value))
            return IGNORE;

        throw new IllegalArgumentException("Illegal conflicting packets policy, can be either '"
                + OVERRIDE_MODE + "' or '" + IGNORE_MODE + "'");
    }

    public boolean isOverride() {
        return this == OVERRIDE;
    }
}
