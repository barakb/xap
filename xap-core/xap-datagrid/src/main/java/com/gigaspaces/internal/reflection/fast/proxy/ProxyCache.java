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

package com.gigaspaces.internal.reflection.fast.proxy;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds already created proxies.
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ProxyCache {
    /**
     * marks that a particular proxy class is currently being generated
     */
    final private static WeakReference<Class> PENDING_GENERATION_MARKER = new WeakReference<Class>(null);

    /**
     * maps a class loader to the proxy class cache for that loader
     */
    final private static Map<Long, Map<InterfacesKey, WeakReference<Class>>> loaderToCache =
            new HashMap<Long, Map<InterfacesKey, WeakReference<Class>>>();

    //Keep a strong reference to the listener to avoid the ClassLoaderCache from removing this listener
    //since it keeps it as a weak reference
    final static ClassLoaderCacheListener cacheStateListener = new ClassLoaderCacheListener();

    static {
        ClassLoaderCache.getCache().registerCacheStateListener(cacheStateListener);
    }

    private static class ClassLoaderCacheListener implements IClassLoaderCacheStateListener {

        public void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
            synchronized (loaderToCache) {
                loaderToCache.remove(classLoaderKey);
            }
        }
    }

    static private class InterfacesKey {
        private final Class<?>[] _interfaces;

        public InterfacesKey(Class<?>[] interfaces) {
            _interfaces = interfaces;
        }

        @Override
        public int hashCode() {
            if (_interfaces.length > 0)
                return _interfaces[0].hashCode();

            return 432543;
        }

        @Override
        public boolean equals(Object obj) {
            InterfacesKey other = (InterfacesKey) obj;

            return Arrays.equals(_interfaces, other._interfaces);
        }
    }

    public void add(ClassLoader loader, Class<?>[] interfaces, Class proxyClass) {
        synchronized (loaderToCache) {
            Long classLoaderKey = ClassLoaderCache.getCache().getClassLoaderKey(loader);
            Map<InterfacesKey, WeakReference<Class>> cache = loaderToCache.get(classLoaderKey);
            synchronized (cache) {
                InterfacesKey key = new InterfacesKey(interfaces);
                cache.put(key, new WeakReference<Class>(proxyClass));
                cache.notifyAll();
            }
        }
    }

    public Class findInCache(ClassLoader loader, Class<?>[] interfaces) {
        /*
         * Find or create the proxy class cache for the class loader.
		 */
        Map<InterfacesKey, WeakReference<Class>> cache;
        synchronized (loaderToCache) {
            Long classLoaderKey = ClassLoaderCache.getCache().putClassLoader(loader);
            cache = loaderToCache.get(classLoaderKey);
            if (cache == null) {
                cache = new HashMap<InterfacesKey, WeakReference<Class>>();
                loaderToCache.put(classLoaderKey, cache);
            }
			/*
			 * This mapping will remain valid for the duration of this
			 * method, without further synchronization, because the mapping
			 * will only be removed if the class loader becomes unreachable.
			 */
        }

        InterfacesKey key = new InterfacesKey(interfaces);
		/*
		 * Look up the list of interfaces in the proxy class cache using
		 * the key.  This lookup will result in one of three possible
		 * kinds of values:
		 *     null, if there is currently no proxy class for the list of
		 *         interfaces in the class loader,
		 *     the pendingGenerationMarker object, if a proxy class for the
		 *         list of interfaces is currently being generated,
		 *     or a weak reference to a Class object, if a proxy class for
		 *         the list of interfaces has already been generated.
		 */
        synchronized (cache) {
			/*
			 * Note that we need not worry about reaping the cache for
			 * entries with cleared weak references because if a proxy class
			 * has been garbage collected, its class loader will have been
			 * garbage collected as well, so the entire cache will be reaped
			 * from the loaderToCache map.
			 */
            do {
                WeakReference<Class> proxyRef = cache.get(key);
                Class proxyClass = null;
                if (proxyRef != null)
                    proxyClass = proxyRef.get();
                if (proxyClass != null) {
                    // proxy class already generated: return it
                    return proxyClass;
                }

                if (proxyRef == PENDING_GENERATION_MARKER) {
                    // proxy class being generated: wait for it
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting to find a proxy in cache.", e);
                    }
                    continue;
                }
					
				/*
				 * No proxy class for this list of interfaces has been
				 * generated or is being generated, so we will go and
				 * generate it now.  Mark it as pending generation.
				 */
                cache.put(key, PENDING_GENERATION_MARKER);
                return null;

            } while (true);
        }
    }
}
