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

import java.util.HashSet;
import java.util.Set;

/**
 * Represents all the possible operation types that can be replicated.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.02
 */
public enum ReplicationOperationType {
    WRITE(false), TAKE(true), EXTEND_LEASE(false), UPDATE(false), DISCARD(false),
    LEASE_EXPIRATION(false), NOTIFY(true), TRANSACTION(false), EVICT(true), CHANGE(false);

    static final private ReplicationOperationType[] _values;

    private boolean template;

    final public static Set<ReplicationOperationType> FULL_PERMISSIONS = new HashSet<ReplicationOperationType>();

    ReplicationOperationType(boolean template) {
        this.template = template;
    }

    static {
        ReplicationOperationType[] orig = values();
        _values = new ReplicationOperationType[orig.length];
        System.arraycopy(orig, 0, _values, 0, orig.length);

        FULL_PERMISSIONS.add(WRITE);
        FULL_PERMISSIONS.add(TAKE);
        FULL_PERMISSIONS.add(EXTEND_LEASE);
        FULL_PERMISSIONS.add(UPDATE);
        FULL_PERMISSIONS.add(LEASE_EXPIRATION);
        FULL_PERMISSIONS.add(NOTIFY);
        FULL_PERMISSIONS.add(TRANSACTION);
        FULL_PERMISSIONS.add(EVICT);
        FULL_PERMISSIONS.add(CHANGE);
    }

    public byte getOpCode() {
        return (byte) ordinal();
    }

    public boolean isTemplate() {
        return template;
    }

    public static ReplicationOperationType fromByte(byte opCode) {
        return _values[opCode];
    }
}
