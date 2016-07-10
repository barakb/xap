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

package com.gigaspaces.internal.os.jmx;

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.os.OSStatisticsProbe;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JMXOSStatisticsProbe implements OSStatisticsProbe {

    private static final OperatingSystemMXBean operatingSystemMXBean;

    private static Method getCommittedVirtualMemorySize;
    private static Method getFreeSwapSpaceSize;
    private static Method getFreePhysicalMemorySize;

    static {
        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

        try {
            Class sunOperatingSystemMXBeanClass = JMXOSDetailsProbe.class.getClassLoader().loadClass("com.sun.management.OperatingSystemMXBean");

            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getCommittedVirtualMemorySize");
                method.setAccessible(true);
                getCommittedVirtualMemorySize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getFreeSwapSpaceSize");
                method.setAccessible(true);
                getFreeSwapSpaceSize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getFreePhysicalMemorySize");
                method.setAccessible(true);
                getFreePhysicalMemorySize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
        } catch (ClassNotFoundException e) {
            // not using sun. can't get the information
        }
    }

    public OSStatistics probeStatistics() {
        long committedVirtualMemorySize = -1;
        if (getCommittedVirtualMemorySize != null) {
            try {
                committedVirtualMemorySize = (Long) getCommittedVirtualMemorySize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }
        long freeSwapSpaceSize = -1;
        if (getFreeSwapSpaceSize != null) {
            try {
                freeSwapSpaceSize = (Long) getFreeSwapSpaceSize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }
        long freePhysicalMemorySize = -1;
        if (getFreePhysicalMemorySize != null) {
            try {
                freePhysicalMemorySize = (Long) getFreePhysicalMemorySize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }

        return new OSStatistics(System.currentTimeMillis(), freeSwapSpaceSize, freePhysicalMemorySize);
    }
}
