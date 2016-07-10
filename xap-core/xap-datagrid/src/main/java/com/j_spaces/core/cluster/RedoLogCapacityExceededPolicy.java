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
 * Dictates which action to take when the redo log capacity is exceeded
 *
 * @author eitany
 * @since 7.1
 */
public enum RedoLogCapacityExceededPolicy {
    /**
     * Block operations until redo log size will decrease
     */
    BLOCK_OPERATIONS,
    /**
     * Drop oldest pending item in the redo log
     */
    DROP_OLDEST;

    private static final int CODE_BLOCK_OPERATIONS = 1;
    private static final int CODE_DROP_OLDEST = 2;

    public static RedoLogCapacityExceededPolicy fromCode(Integer code) {
        if (code == null)
            return null;

        switch (code) {
            case CODE_BLOCK_OPERATIONS:
                return BLOCK_OPERATIONS;
            case CODE_DROP_OLDEST:
                return DROP_OLDEST;
            default:
                throw new IllegalArgumentException("Unsupported enum code: " + code);
        }
    }
}
