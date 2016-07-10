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

package com.gigaspaces.internal.utils.concurrent;


/**
 * Decorates a runnable with the property that the execution of the runnable will be made with the
 * specified class loader
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class RunnableContextClassLoaderDecorator implements Runnable {
    private final ClassLoader executingClassLoader;
    private final Runnable command;

    public RunnableContextClassLoaderDecorator(ClassLoader executingClassLoader,
                                               Runnable command) {
        if (command == null)
            throw new IllegalArgumentException("command cannot be null");
        this.executingClassLoader = executingClassLoader;
        this.command = command;
    }

    public void run() {
        //Change context class loader if needed
        Thread t = Thread.currentThread();
        ClassLoader current = t.getContextClassLoader();
        t.setContextClassLoader(executingClassLoader);
        try {
            command.run();
        } finally {
            t.setContextClassLoader(current);
        }
    }
}