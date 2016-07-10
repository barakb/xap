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

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A cache to share async handler providers within the same context (service class loader/processing
 * unit instance)
 *
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SharedHandlerProviderCache implements IClassLoaderCacheStateListener {

    private static final SharedHandlerProviderCache INSTANCE_SHARED_POOL = new SharedHandlerProviderCache(new IAsyncHandlerProviderFactory() {
        // TODO durable notifications : configurable?
        private final int corePoolSize = Runtime.getRuntime().availableProcessors();

        @Override
        public IAsyncHandlerProvider createProvider(String name) {
            return new ScheduledThreadPoolAsyncHandlerProvider("SharedScheduledPoolAsyncHandlerProvider-" + name, corePoolSize);
        }
    });

    public static SharedHandlerProviderCache getSharedThreadPoolProviderCache() {
        return INSTANCE_SHARED_POOL;
    }

    private final Map<Long, SharedAsyncHandlerProvider> _cachedAsyncHandlers = new HashMap<Long, SharedAsyncHandlerProvider>();
    private final IAsyncHandlerProviderFactory _factory;
    private final Object _lock = new Object();

    private SharedHandlerProviderCache(IAsyncHandlerProviderFactory factory) {
        _factory = factory;
        ClassLoaderCache.getCache().registerCacheStateListener(this);
    }

    public IAsyncHandlerProvider getProvider() {
        synchronized (_lock) {
            ClassLoader contextClassLoader = ClassLoaderHelper.getContextClassLoader();
            Long classLoaderKey = null;
            if (contextClassLoader != null)
                classLoaderKey = ClassLoaderCache.getCache().putClassLoader(contextClassLoader);

            SharedAsyncHandlerProvider handlerProvider = _cachedAsyncHandlers.get(classLoaderKey);
            if (handlerProvider == null) {
                handlerProvider = createNewHandlerProvider(ClassLoaderHelper.getClassLoaderLogName(contextClassLoader), classLoaderKey);
                _cachedAsyncHandlers.put(classLoaderKey, handlerProvider);
            } else {
                handlerProvider.addReference();
            }

            return handlerProvider.getWrapper();
        }
    }

    private SharedAsyncHandlerProvider createNewHandlerProvider(String name, Long classLoaderKey) {
        IAsyncHandlerProvider asyncHandlerProvider = _factory.createProvider(name);
        return new SharedAsyncHandlerProvider(asyncHandlerProvider, classLoaderKey);
    }

    @Override
    public void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
        synchronized (_lock) {
            SharedAsyncHandlerProvider sharedAsyncHandlerProvider = _cachedAsyncHandlers.remove(classLoaderKey);
            if (sharedAsyncHandlerProvider != null) {
                IAsyncHandlerProvider asyncHandlerProvider = sharedAsyncHandlerProvider.getUnderlyingHandler();
                asyncHandlerProvider.close();
            }
        }
    }

    private class SharedAsyncHandlerProvider {

        private final IAsyncHandlerProvider _asyncHandlerProvider;
        private int _referenceCounter = 1;
        private final Long _classLoaderKey;

        public SharedAsyncHandlerProvider(IAsyncHandlerProvider asyncHandlerProvider, Long classLoaderKey) {
            _asyncHandlerProvider = asyncHandlerProvider;
            _classLoaderKey = classLoaderKey;
        }

        public IAsyncHandlerProvider getWrapper() {
            return new SharedAsyncHandlerProviderWrapper(this);
        }

        public IAsyncHandlerProvider getUnderlyingHandler() {
            return _asyncHandlerProvider;
        }

        public IAsyncHandler start(AsyncCallable callable, long idleDelayMilis,
                                   String name, boolean waitIdleDelayBeforeStart) {
            return _asyncHandlerProvider.start(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
        }

        public IAsyncHandler startMayBlock(AsyncCallable callable,
                                           long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart) {
            return _asyncHandlerProvider.startMayBlock(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
        }

        public void close() {
            synchronized (_lock) {
                if (--_referenceCounter == 0) {
                    _cachedAsyncHandlers.remove(_classLoaderKey);
                    _asyncHandlerProvider.close();
                }
            }
        }

        public void addReference() {
            synchronized (_lock) {
                _referenceCounter++;
            }
        }
    }

    public static class SharedAsyncHandlerProviderWrapper extends AbstractAsyncHandlerProvider {

        private final SharedAsyncHandlerProvider _sharedAsyncHandlerProvider;
        private final List<IAsyncHandler> _handlers = new LinkedList<IAsyncHandler>();

        public SharedAsyncHandlerProviderWrapper(SharedAsyncHandlerProvider sharedAsyncHandlerProvider) {
            this._sharedAsyncHandlerProvider = sharedAsyncHandlerProvider;
        }

        @Override
        public IAsyncHandler startImpl(AsyncCallable callable, long idleDelayMilis,
                                       String name, boolean waitIdleDelayBeforeStart) {
            IAsyncHandler asyncHandler = _sharedAsyncHandlerProvider.start(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
            _handlers.add(asyncHandler);
            return asyncHandler;
        }

        @Override
        public IAsyncHandler startMayBlockImpl(AsyncCallable callable,
                                               long idleDelayMilis, String name,
                                               boolean waitIdleDelayBeforeStart) {
            IAsyncHandler asyncHandler = _sharedAsyncHandlerProvider.startMayBlock(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
            _handlers.add(asyncHandler);
            return asyncHandler;
        }

        @Override
        protected void onClose() {
            for (IAsyncHandler handler : _handlers)
                handler.stop(10, TimeUnit.MILLISECONDS);
            _handlers.clear();

            _sharedAsyncHandlerProvider.close();
        }

    }

    public interface IAsyncHandlerProviderFactory {
        IAsyncHandlerProvider createProvider(String name);
    }

}