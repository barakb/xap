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

package com.j_spaces.kernel.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (shay.banon)
 */
@com.gigaspaces.api.InternalApi
public class DynamicThreadFactory implements ThreadFactory {

    static final private AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    final ThreadGroup _group;
    final AtomicInteger _threadNumber = new AtomicInteger(1);
    final String _namePrefix;
    final int _priority;

    public DynamicThreadFactory(String namePrefix, int priority) {
        SecurityManager s = System.getSecurityManager();
        _group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        _namePrefix = namePrefix + "-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        _priority = priority;
    }

    public Thread newThread(Runnable r) {
        DynamicThread t = new DynamicThread(_group, r, _namePrefix + _threadNumber.getAndIncrement(), 0);
        t.setDaemon(true);
        t.setPriority(_priority);
        return t;
    }
}
