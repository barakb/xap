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

package com.gigaspaces.internal.exceptions;

import com.gigaspaces.internal.utils.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class PartitionedExecutionExceptionsCollection {
    private final Map<Integer, Exception> _exceptions;

    public PartitionedExecutionExceptionsCollection() {
        this._exceptions = new HashMap<Integer, Exception>();
    }

    public void add(int partitionId, Exception partitionException) {
        this._exceptions.put(partitionId, partitionException);
    }

    public <T extends RuntimeException> void throwIfAny(Class<T> type) {
        T exception = getIfExists(type);
        if (exception != null)
            throw exception;
    }

    private <T extends Throwable> T getIfExists(Class<T> type) {
        for (Exception exception : _exceptions.values())
            if (type.isInstance(exception))
                return (T) exception;
        return null;
    }

    public String getDescription() {
        if (_exceptions.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Exception> partition : _exceptions.entrySet()) {
            sb.append("Partition #")
                    .append(partition.getKey())
                    .append(": ")
                    .append(partition.getValue())
                    .append(StringUtils.NEW_LINE);
        }
        return sb.toString();
    }
}
