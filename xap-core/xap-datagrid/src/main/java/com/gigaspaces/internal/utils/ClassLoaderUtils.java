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

import java.net.URL;
import java.net.URLClassLoader;

@com.gigaspaces.api.InternalApi
public class ClassLoaderUtils {
    public static String getCurrentClassPathString() {
        return getClassPathString(Thread.currentThread().getContextClassLoader());
    }

    public static String getClassPathString(ClassLoader classLoader) {
        StringBuilder classpath = new StringBuilder();
        while (classLoader != null) {
            classpath.append('{');
            if (classLoader instanceof CustomURLClassLoader) {
                ((CustomURLClassLoader) classLoader).dump(classpath);
            } else {
                classpath.append(classLoader);
                if (classLoader instanceof URLClassLoader) {
                    final URLClassLoader ucl = (URLClassLoader) classLoader;
                    classpath.append(" URLs:" + StringUtils.NEW_LINE);
                    for (URL url : ucl.getURLs()) {
                        classpath.append(url + StringUtils.NEW_LINE);
                    }
                    classpath.append("*** End of URLs ***");
                }
            }
            classpath.append('}');
            classLoader = classLoader == ClassLoader.getSystemClassLoader() ? null : classLoader.getParent();
        }
        return classpath.toString();
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
