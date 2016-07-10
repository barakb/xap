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

package com.gigaspaces.internal.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A utility for generating thread dumps.
 *
 * @author Idan Moyal
 * @since 9.0.2
 */
@com.gigaspaces.api.InternalApi
public class ThreadDumpUtility {

    public static String generateThreadDumpIfPossible() {
        try {
            return generateThreadDump();
        } catch (Exception e) {
            return "Error creating thread dump " + e.getMessage();
        }
    }

    public static String generateThreadDump() throws Exception {
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        StringBuilder dump = new StringBuilder();
        processDeadlocks(dump, threadBean);
        processAllThreads(dump, threadBean);
        return dump.toString();
    }

    private static void processAllThreads(StringBuilder dump, ThreadMXBean threadBean) throws Exception {
        dump.append(StringUtils.NEW_LINE);
        dump.append("===== All Threads =====");
        dump.append(StringUtils.NEW_LINE);
        dumpThreads(dump, dumpAllThreads(threadBean));
    }

    private static ThreadInfo[] dumpAllThreads(ThreadMXBean threadBean) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Method method = threadBean.getClass().getDeclaredMethod("dumpAllThreads", boolean.class, boolean.class);
        method.setAccessible(true);
        return (ThreadInfo[]) method.invoke(threadBean, true, true);
    }

    private static ThreadInfo[] getThreadInfo(long[] threadIds, ThreadMXBean threadBean) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Method method = threadBean.getClass().getDeclaredMethod("getThreadInfo", long[].class, boolean.class, boolean.class);
        method.setAccessible(true);
        return (ThreadInfo[]) method.invoke(threadBean, threadIds, true, true);
    }

    private static long[] findDeadlockedThreads(ThreadMXBean threadBean) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Method method = threadBean.getClass().getDeclaredMethod("findDeadlockedThreads");
        method.setAccessible(true);
        return (long[]) method.invoke(threadBean);
    }

    private static void processDeadlocks(StringBuilder dump, ThreadMXBean threadBean) throws Exception {
        dump.append("=====  Deadlocked Threads =====");
        dump.append(StringUtils.NEW_LINE);
        long deadlockedThreadIds[] = findDeadlockedThreads(threadBean);
        if (deadlockedThreadIds != null)
            dumpThreads(dump, getThreadInfo(deadlockedThreadIds, threadBean));
    }

    private static void dumpThreads(StringBuilder dump, ThreadInfo[] infos) throws Exception {
        for (ThreadInfo info : infos) {
            dump.append(StringUtils.NEW_LINE);
            write(info, dump);
        }

    }

    private static void write(ThreadInfo threadInfo, StringBuilder dump) throws Exception {
        dump.append(String.format("\"%s\" Id=%s %s", threadInfo.getThreadName(), threadInfo.getThreadId(), threadInfo.getThreadState()));
        if (threadInfo.getLockName() != null) {
            dump.append(String.format(" on %s", threadInfo.getLockName()));
            if (threadInfo.getLockOwnerName() != null)
                dump.append(String.format(" owned by \"%s\" Id=%s", threadInfo.getLockOwnerName(), threadInfo.getLockOwnerId()));
        }
        if (threadInfo.isInNative())
            dump.append(" (in native)");
        dump.append(StringUtils.NEW_LINE);

        Class<?> monitorInfoClass = Class.forName("java.lang.management.MonitorInfo", true, ThreadDumpUtility.class.getClassLoader());
        Method getLockedStackFrameMethod = monitorInfoClass.getMethod("getLockedStackFrame");
        getLockedStackFrameMethod.setAccessible(true);
        Method getClassNameMethod = monitorInfoClass.getMethod("getClassName");
        getClassNameMethod.setAccessible(true);
        Method getIdentityHashCodeMethod = monitorInfoClass.getMethod("getIdentityHashCode");
        getIdentityHashCodeMethod.setAccessible(true);

        Method lockedMonitorsMethod = ThreadInfo.class.getDeclaredMethod("getLockedMonitors");
        lockedMonitorsMethod.setAccessible(true);
        Object[] lockedMonitors = (Object[]) lockedMonitorsMethod.invoke(threadInfo);
        StackTraceElement stackTraceElements[] = threadInfo.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTraceElements) {
            dump.append("    at " + stackTraceElement);
            dump.append(StringUtils.NEW_LINE);
            Object lockedMonitor = findLockedMonitor(stackTraceElement, lockedMonitors, getLockedStackFrameMethod);
            if (lockedMonitor != null) {
                dump.append(("    - locked " + getClassNameMethod.invoke(lockedMonitor) + "@" + getIdentityHashCodeMethod.invoke(lockedMonitor)));
                dump.append(StringUtils.NEW_LINE);
            }
        }
    }

    private static Object findLockedMonitor(StackTraceElement stackTraceElement,
                                            Object[] lockedMonitors, Method getLockedStackFrameMethod) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        for (Object monitorInfo : lockedMonitors) {
            if (stackTraceElement.equals(getLockedStackFrameMethod.invoke(monitorInfo)))
                return monitorInfo;
        }
        return null;
    }


}
