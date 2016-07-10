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

package com.gigaspaces.cluster.replication;

/**
 * Specifies consistency level for a modifying (replicated) operation
 *
 * @author eitany
 * @since 9.5.1
 */
public enum ConsistencyLevel {
    /**
     * Ensure that the modification has been updated in at least 1 member before responding to the
     * client, otherwise the modification is rejected and a {@link ConsistencyLevelViolationException}
     * is thrown. In a primary backup topology that member is the primary.
     */
    ANY((byte) 0),
    /**
     * Ensure that the modification has been updated in at least N / 2 + 1 members before responding
     * to the client, otherwise the modification is rejected and a {@link
     * ConsistencyLevelViolationException} is thrown. In a primary backup topology one of the
     * members is the primary.
     */
    QUORUM((byte) 1),
    /**
     * Ensure that the modification has been updated in all members before responding to the client,
     * otherwise the modification is rejected and a {@link ConsistencyLevelViolationException} is
     * thrown.
     */
    ALL((byte) 2);

    private byte _code;

    private ConsistencyLevel(byte code) {
        _code = code;
    }

    public byte getCode() {
        return _code;
    }

    public static ConsistencyLevel fromCode(byte readByte) {
        switch (readByte) {
            case 0:
                return ANY;
            case 1:
                return QUORUM;
            case 2:
                return ALL;
            default:
                throw new IllegalArgumentException("illegal code " + readByte);
        }
    }
}
