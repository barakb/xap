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

package com.gigaspaces.internal.cluster;

/**
 * @author Niv Ingberg
 * @since 9.1.2
 */
public enum SpaceProxyLoadBalancerType {
    STICKY(1),
    ROUND_ROBIN(2);

    private final byte code;

    SpaceProxyLoadBalancerType(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static SpaceProxyLoadBalancerType fromCode(byte code) {
        switch (code) {
            case 1:
                return STICKY;
            case 2:
                return ROUND_ROBIN;
            default:
                throw new IllegalStateException("Unsupported code " + code);
        }
    }
}
