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

package com.gigaspaces.internal.jvm.jmx;

import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.jvm.JVMStatisticsProbe;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JMXJVMStatisticsProbe implements JVMStatisticsProbe {

    private static RuntimeMXBean runtimeMXBean;

    private static MemoryMXBean memoryMXBean;

    private static ThreadMXBean threadMXBean;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
    }

    public JVMStatistics probeStatistics() {
        long gcCollectionCount = 0;
        long gcCollectionTime = 0;
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            long tmp = gcMxBean.getCollectionCount();
            if (tmp != -1) {
                gcCollectionCount += tmp;
            }
            tmp = gcMxBean.getCollectionTime();
            if (tmp != -1) {
                gcCollectionTime += tmp;
            }
        }
        return new JVMStatistics(System.currentTimeMillis(), runtimeMXBean.getUptime(),
                memoryMXBean.getHeapMemoryUsage().getCommitted(), memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getNonHeapMemoryUsage().getCommitted(), memoryMXBean.getNonHeapMemoryUsage().getUsed(),
                threadMXBean.getThreadCount(), threadMXBean.getPeakThreadCount(), gcCollectionCount, gcCollectionTime);
    }
}