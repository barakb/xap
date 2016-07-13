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

package com.j_spaces.core;

/**
 * An interface for retrieving current JVM memory details & provides the ability to perform GC.
 *
 * @author idan
 * @since 9.5
 */
public interface IProcessMemoryManager {
    void performGC();

    double getMemoryUsagePercentage(boolean asyncCheckIfEnabled);

    long getMemoryUsage();

    long getMaximumMemory();

    long getFreeMemory();

    boolean isAsyncCheckEnabled();
}
