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

package com.gigaspaces.admin.ui;

import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.threadpool.DynamicExecutors;

import java.util.concurrent.ExecutorService;

/**
 * A shared admin thread pool.
 *
 * @author kimchy
 */
public abstract class AdminUIThreadPool {

    private static final ExecutorService _threadPool;

    static {
        //thread pool initialization
        int minThreadPoolSize = Integer.parseInt(System.getProperty(
                SystemProperties.UI_THREAD_POOL_MIN_SIZE,
                SystemProperties.UI_THREAD_POOL_MIN_SIZE_DEFAULT_VALUE));
        int maxThreadPoolSize = Integer.parseInt(System.getProperty(
                SystemProperties.UI_THREAD_POOL_MAX_SIZE,
                SystemProperties.UI_THREAD_POOL_MAX_SIZE_DEFAULT_VALUE));
        int keepAliveTime = Integer.parseInt(
                System.getProperty(SystemProperties.UI_THREAD_POOL_KEEP_ALIVE_TIME,
                        SystemProperties.UI_THREAD_POOL_KEEP_ALIVE_TIME_DEFAULT_VALUE)) * 1000; //( seconds to milliseconds)

        _threadPool = DynamicExecutors.newScalingThreadPool(minThreadPoolSize,
                maxThreadPoolSize,
                keepAliveTime,
                DynamicExecutors.daemonThreadFactory("Admin"));
    }

    private AdminUIThreadPool() {

    }

    /**
     * Returns ThreadPool that used in order to run code that executed on receiving Jini events
     * within separate threads.
     *
     * @return _threadPool
     */
    public static ExecutorService getThreadPool() {
        return _threadPool;
    }


}
