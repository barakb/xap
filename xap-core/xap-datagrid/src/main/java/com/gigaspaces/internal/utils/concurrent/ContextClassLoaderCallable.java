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

import java.util.concurrent.Callable;

/**
 * Base class for Callable implementations which should run in a specific class loader.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */
public abstract class ContextClassLoaderCallable<T> implements Callable<T> {
    private final ClassLoader _contextClassLoader;

    public ContextClassLoaderCallable() {
        this._contextClassLoader = Thread.currentThread().getContextClassLoader();
    }

    public ContextClassLoaderCallable(ClassLoader contextClassLoader) {
        this._contextClassLoader = contextClassLoader;
    }

    @Override
    public T call() throws Exception {
        Thread executionThread = Thread.currentThread();
        ClassLoader executionClassLoader = executionThread.getContextClassLoader();
        final boolean wasChanged = (executionClassLoader != _contextClassLoader);
        if (wasChanged)
            executionThread.setContextClassLoader(_contextClassLoader);

        try {
            return execute();
        } finally {
            if (wasChanged)
                executionThread.setContextClassLoader(executionClassLoader);
        }
    }

    protected abstract T execute() throws Exception;
}
