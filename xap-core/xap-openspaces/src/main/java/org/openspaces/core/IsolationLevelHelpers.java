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

package org.openspaces.core;

import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.ReadModifiers;

import org.springframework.transaction.TransactionDefinition;

/**
 * Utility methods to handle conversions between different isolation level representations.
 *
 * @author Dan Kilman
 * @since 9.5
 */
public class IsolationLevelHelpers {

    /**
     * @param springIsolationLevel the spring isolation level as defined in {@link
     *                             TransactionDefinition}.
     * @param defaultValue         The modifiers to use in case springIsolationLevel equals {@link
     *                             TransactionDefinition#ISOLATION_DEFAULT}
     * @return A matcing GigaSpaces isolation level modifier.
     * @see TransactionDefinition
     * @see ReadModifiers
     * @see CountModifiers
     */
    public static int convertSpringToSpaceIsolationLevel(int springIsolationLevel, int defaultValue) {
        switch (springIsolationLevel) {
            case TransactionDefinition.ISOLATION_DEFAULT:
                return defaultValue;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
                return ReadModifiers.DIRTY_READ.getCode();
            case TransactionDefinition.ISOLATION_READ_COMMITTED:
                return ReadModifiers.READ_COMMITTED.getCode();
            case TransactionDefinition.ISOLATION_REPEATABLE_READ:
                return ReadModifiers.REPEATABLE_READ.getCode();
            default:
                throw new IllegalArgumentException("GigaSpace does not support isolation level [" + springIsolationLevel + "]");
        }
    }

    /**
     * @param isolationLevel The isolation level code number.
     * @return a {@link ReadModifiers} instance representing the isolationLevel parameter
     */
    public static ReadModifiers toReadModifiers(int isolationLevel) {
        if (isolationLevel == ReadModifiers.NONE.getCode()) {
            return ReadModifiers.NONE;
        } else if (isolationLevel == ReadModifiers.DIRTY_READ.getCode()) {
            return ReadModifiers.DIRTY_READ;
        } else if (isolationLevel == ReadModifiers.READ_COMMITTED.getCode()) {
            return ReadModifiers.READ_COMMITTED;
        } else if (isolationLevel == ReadModifiers.REPEATABLE_READ.getCode()) {
            return ReadModifiers.REPEATABLE_READ;
        } else {
            throw new IllegalArgumentException("GigaSpace does not support isolation level: " + isolationLevel);
        }
    }

    /**
     * @param isolationLevel The isolation level code number.
     * @return a {@link CountModifiers} instance representing the isolationLevel parameter
     */
    public static CountModifiers toCountModifiers(int isolationLevel) {
        if (isolationLevel == CountModifiers.NONE.getCode()) {
            return CountModifiers.NONE;
        } else if (isolationLevel == CountModifiers.DIRTY_READ.getCode()) {
            return CountModifiers.DIRTY_READ;
        } else if (isolationLevel == CountModifiers.READ_COMMITTED.getCode()) {
            return CountModifiers.READ_COMMITTED;
        } else if (isolationLevel == CountModifiers.REPEATABLE_READ.getCode()) {
            return CountModifiers.REPEATABLE_READ;
        } else {
            throw new IllegalArgumentException("GigaSpace does not support isolation level: " + isolationLevel);
        }
    }

    /**
     * @param modifiers The modifiers to merge the isolation level with.
     * @param gigaSpace The {@link GigaSpace} instance to test the current isolation level with.
     * @return The merged modifiers (i.e, if there is a current transaction defined and it has a non
     * default isolation level set, it will replace any currently set isolation level on the
     * modifiers argument.
     */
    public static ReadModifiers mergeWithIsolationLevelModifiersIfNeeded(ReadModifiers modifiers, GigaSpace gigaSpace) {

        int springIsolationLevel = gigaSpace.getTxProvider().getCurrentTransactionIsolationLevel(gigaSpace);

        ReadModifiers actualIsolationLevel;

        switch (springIsolationLevel) {
            case TransactionDefinition.ISOLATION_DEFAULT:
                return modifiers;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
                actualIsolationLevel = ReadModifiers.DIRTY_READ;
                break;
            case TransactionDefinition.ISOLATION_READ_COMMITTED:
                actualIsolationLevel = ReadModifiers.READ_COMMITTED;
                break;
            case TransactionDefinition.ISOLATION_REPEATABLE_READ:
                actualIsolationLevel = ReadModifiers.REPEATABLE_READ;
                break;
            default:
                throw new IllegalArgumentException("GigaSpace does not support isolation level [" + springIsolationLevel + "]");
        }

        return modifiers.setIsolationLevel(actualIsolationLevel);
    }

    /**
     * @param modifiers The modifiers to merge the isolation level with.
     * @param gigaSpace The {@link GigaSpace} instance to test the current isolation level with.
     * @return The merged modifiers (i.e, if there is a current transaction defined and it has a non
     * default isolation level set, it will replace any currently set isolation level on the
     * modifiers argument.
     */
    public static CountModifiers mergeWithIsolationLevelModifiersIfNeeded(CountModifiers modifiers, GigaSpace gigaSpace) {

        int springIsolationLevel = gigaSpace.getTxProvider().getCurrentTransactionIsolationLevel(gigaSpace);

        CountModifiers actualIsolationLevel;

        switch (springIsolationLevel) {
            case TransactionDefinition.ISOLATION_DEFAULT:
                return modifiers;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
                actualIsolationLevel = CountModifiers.DIRTY_READ;
                break;
            case TransactionDefinition.ISOLATION_READ_COMMITTED:
                actualIsolationLevel = CountModifiers.READ_COMMITTED;
                break;
            case TransactionDefinition.ISOLATION_REPEATABLE_READ:
                actualIsolationLevel = CountModifiers.REPEATABLE_READ;
                break;
            default:
                throw new IllegalArgumentException("GigaSpace does not support isolation level [" + springIsolationLevel + "]");
        }

        return modifiers.setIsolationLevel(actualIsolationLevel);
    }

}
