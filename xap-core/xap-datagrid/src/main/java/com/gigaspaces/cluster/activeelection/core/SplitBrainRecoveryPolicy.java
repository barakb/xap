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

package com.gigaspaces.cluster.activeelection.core;

/**
 * Created by moran on 4/12/15.
 *
 * @since 10.2.0
 */
public enum SplitBrainRecoveryPolicy {

    DISCARD_LEAST_CONSISTENT,
    SUSPEND_PARTITION_PRIMARIES;

    public static SplitBrainRecoveryPolicy getEnum(String value) {
        String newValue = value.toUpperCase().replaceAll("-", "_");
        try {
            return SplitBrainRecoveryPolicy.valueOf(newValue);
        } catch (IllegalArgumentException illegalEx) {
            throw new IllegalArgumentException("Illegal split-brain recovery policy [" + value + "]", illegalEx);
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase().replaceAll("_", "-");
    }
}
