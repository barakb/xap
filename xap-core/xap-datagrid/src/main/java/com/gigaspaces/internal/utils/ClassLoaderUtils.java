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

import org.jini.rio.boot.CustomURLClassLoader;
import org.jini.rio.boot.LoggableClassLoader;

import java.net.URL;
import java.net.URLClassLoader;

@com.gigaspaces.api.InternalApi
public class ClassLoaderUtils {
    public static String getCurrentClassPathString(String prefix) {
        return getClassPathString(Thread.currentThread().getContextClassLoader(), prefix);
    }

    public static String getClassPathString(ClassLoader classLoader, String prefix) {
        StringBuilder classpath = new StringBuilder();
        appendClassLoader(classLoader, classpath, prefix);
        return classpath.toString();
    }

    private static void appendClassLoader(ClassLoader classLoader, StringBuilder classpath, String prefix) {
        if (classLoader == null)
            return;

        classpath.append(prefix + ": " + getClassLoaderName(classLoader));
        if (classLoader instanceof URLClassLoader) {
            final URL[] urls = ((URLClassLoader) classLoader).getURLs();
            classpath.append(", Urls: " + urls.length  + StringUtils.NEW_LINE);
            for (URL url : urls) {
                classpath.append("    " + url + StringUtils.NEW_LINE);
            }
        }
        if (classLoader != ClassLoader.getSystemClassLoader())
            appendClassLoader(classLoader.getParent(), classpath, "Parent Class Loader");
    }

    private static String getClassLoaderName(ClassLoader classLoader) {
        if (classLoader instanceof LoggableClassLoader) {
            return ((LoggableClassLoader)classLoader).getLogName();
        }
        if (ClassLoader.getSystemClassLoader().equals(classLoader)) {
            return "System class Loader";
        }
        return classLoader.toString();
    }

    public static boolean isClassLoaderProblem(Throwable t) {
        boolean classLoaderProblem = false;
        for (; !classLoaderProblem && t != null; t = t.getCause()) {
            if (t instanceof NoClassDefFoundError) {
                classLoaderProblem = true;
            }
        }
        return classLoaderProblem;
    }
}
