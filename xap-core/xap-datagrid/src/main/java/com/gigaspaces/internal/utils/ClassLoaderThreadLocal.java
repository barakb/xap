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

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;

/**
 * Holds thread local per class loader, get and set method will operate on the thread local which is
 * associated to the context class loader. Once the class loader is removed, the associated thread
 * local map is removed as well. This works only
 *
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ClassLoaderThreadLocal<T> implements IClassLoaderCacheStateListener {

    private final CopyOnUpdateMap<Long, ThreadLocal<T>> _classLoaderThreadLocalMap;

    public ClassLoaderThreadLocal() {
        _classLoaderThreadLocalMap = new CopyOnUpdateMap<Long, ThreadLocal<T>>();
        ClassLoaderCache.getCache().registerCacheStateListener(this);
    }

    /**
     * Returns the current thread's "initial value" for this thread-local variable.  This method
     * will be invoked the first time a thread accesses the variable with the {@link #get} method,
     * unless the thread previously invoked the {@link #set} method, in which case the
     * <tt>initialValue</tt> method will not be invoked for the thread.  Normally, this method is
     * invoked at most once per thread, but it may be invoked again in case of subsequent
     * invocations of {@link #remove} followed by {@link #get}.
     *
     * <p>This implementation simply returns <tt>null</tt>; if the programmer desires thread-local
     * variables to have an initial value other than <tt>null</tt>, <tt>ThreadLocal</tt> must be
     * subclassed, and this method overridden.  Typically, an anonymous inner class will be used.
     *
     * @return the initial value for this thread-local
     */
    protected T initialValue() {
        return null;
    }

    /**
     * Returns the value in the current thread's copy of this thread-local variable within the
     * context class loader.  If the variable has no value for the current thread, it is first
     * initialized to the value returned by an invocation of the {@link #initialValue} method.
     *
     * @return the current thread's value of this thread-local
     */
    public T get() {
        ThreadLocal<T> threadLocal = getContextClassLoaderThreadLocal();
        return threadLocal.get();
    }

    /**
     * Sets the current thread's copy of this thread-local variable within the context class loader
     * to the specified value.  Most subclasses will have no need to override this method, relying
     * solely on the {@link #initialValue} method to set the values of thread-locals.
     *
     * @param value the value to be stored in the current thread's copy of this thread-local.
     */
    public void set(T value) {
        ThreadLocal<T> threadLocal = getContextClassLoaderThreadLocal();
        threadLocal.set(value);
    }

    private ThreadLocal<T> getContextClassLoaderThreadLocal() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        // Get class loader key:
        final Long classLoaderKey = classLoader != null
                ? ClassLoaderCache.getCache().putClassLoader(classLoader)
                : -1;

        // Get Class loader map:
        ThreadLocal<T> threadLocal = this._classLoaderThreadLocalMap.get(classLoaderKey);
        if (threadLocal == null) {
            ThreadLocal<T> temp = new ThreadLocal<T>() {
                @Override
                protected T initialValue() {
                    return ClassLoaderThreadLocal.this.initialValue();
                }
            };
            threadLocal = _classLoaderThreadLocalMap.putIfAbsent(classLoaderKey,
                    temp);
            if (threadLocal == null)
                threadLocal = temp;
        }
        return threadLocal;
    }

    @Override
    public void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
        _classLoaderThreadLocalMap.remove(classLoaderKey);
    }
}
