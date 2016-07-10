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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.annotation.pojo.FifoSupport;

/**
 * Helper class to map from binary fifo to tri-state fifo.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class FifoHelper {
    private static final byte DEFAULT_CODE = 0;
    private static final byte OFF_CODE = 1;
    private static final byte OPERATION_CODE = 2;
    private static final byte ALL_CODE = 3;

    /**
     * Private constructor to prevent instantiation of this class.
     */
    private FifoHelper() {
    }

    public static byte toCode(FifoSupport fifoSupport) {
        switch (fifoSupport) {
            case NOT_SET:
                return DEFAULT_CODE;
            case DEFAULT:
                return DEFAULT_CODE;
            case OFF:
                return OFF_CODE;
            case OPERATION:
                return OPERATION_CODE;
            case ALL:
                return ALL_CODE;
            default:
                throw new IllegalArgumentException("Unsupported fifo support: " + fifoSupport);
        }
    }

    public static FifoSupport fromCode(byte code) {
        switch (code) {
            case DEFAULT_CODE:
                return FifoSupport.DEFAULT;
            case OFF_CODE:
                return FifoSupport.OFF;
            case OPERATION_CODE:
                return FifoSupport.OPERATION;
            case ALL_CODE:
                return FifoSupport.ALL;
            default:
                throw new IllegalArgumentException("Unsupported fifo support code: " + code);
        }
    }

    /**
     * This method is used to translate from 'Old' binary fifo to new tri-state fifo. false means no
     * fifo, true means always.
     */
    public static FifoSupport fromOld(boolean isFifo) {
        return isFifo ? FifoSupport.ALL : FifoSupport.OFF;
    }
}
