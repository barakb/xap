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

import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @author idan
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class ProcessMemoryManager implements IProcessMemoryManager {
    private static final Runtime _runtime = Runtime.getRuntime();

    private static final long _totalMemory = (_runtime.totalMemory() == _runtime.maxMemory()) ? _runtime.totalMemory() : -1;
    private static final long _maximumMemory = _runtime.maxMemory();
    private ExecutorService executorService;
    private FutureTask<Long> inProcess;

    public static final IProcessMemoryManager INSTANCE = new ProcessMemoryManager();

    private ProcessMemoryManager() {
        executorService = Executors.newSingleThreadExecutor(new GSThreadFactory("ProcessMemoryManager", true));
        inProcess = null;
    }

    public void performGC() {
        _runtime.gc();
    }

    public double getMemoryUsagePercentage() {
        return (getMemoryUsage() * 100.0) / getMaximumMemory();
    }

    public long getMemoryUsage() {
        long totalMemory = _totalMemory == -1 ? _runtime.totalMemory() : _totalMemory;
        return totalMemory - getFreeMemory();
    }

    public long getMaximumMemory() {
        return _maximumMemory;
    }

    public long getFreeMemory() {
        FutureTask<Long> futureTask = new FutureTask<Long>(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                try {
                    return _runtime.freeMemory();
                }finally{
                    synchronized (ProcessMemoryManager.this){
                        inProcess = null;
                    }
                }
            }
        });
        try {
            return getRunningFreeMemoryProb(futureTask).get();
        }catch (Exception e){
            return _totalMemory == -1 ? _runtime.totalMemory() : _totalMemory;
        }
    }

    private synchronized Future<Long> getRunningFreeMemoryProb(FutureTask<Long> futureTask) {
        if(inProcess != null){
            return inProcess;
        }else{
            inProcess = futureTask;
            executorService.submit(futureTask);
            return futureTask;
        }
    }

}
