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


package org.openspaces.remoting.scripting.cache;

import org.openspaces.remoting.scripting.LocalScriptExecutor;
import org.openspaces.remoting.scripting.Script;
import org.openspaces.remoting.scripting.ScriptCompilationException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An LRU cache that can handle compiled scripts that are not thread safe. Non thread safe compiled
 * scripts are scripts that can not be executed by more than one thread at once.
 *
 * <p>The cache holds a {@link org.openspaces.remoting.scripting.cache.CompiledScriptPool} per
 * script name, and allocates a pool when a new one is required. The pool allows to get and put
 * compiled scripts.
 *
 * <p>Uses a simple {@link java.util.LinkedHashMap} to implement the LRU.
 *
 * @author kimchy
 */
public class LRUNonThreadSafeCompiledScriptCache implements CompiledScriptCache {

    public static final int DEFAULT_CACHE_SIZE = 50;

    private final int size;

    private CompiledScriptPoolFactory compiledScriptPoolFactory;

    private final LinkedHashMap<String, CompiledScriptPool> cache;

    private final Object mutex = new Object();

    private Map.Entry evicted;

    public LRUNonThreadSafeCompiledScriptCache() {
        this(DEFAULT_CACHE_SIZE);
    }

    public LRUNonThreadSafeCompiledScriptCache(int cacheSize) {
        this(cacheSize, new BlockingQueueCompiledScriptPoolFactory());
    }

    public LRUNonThreadSafeCompiledScriptCache(int cacheSize, CompiledScriptPoolFactory compiledScriptPoolFactory) {
        this.size = cacheSize;
        this.compiledScriptPoolFactory = compiledScriptPoolFactory;
        cache = new LinkedHashMap<String, CompiledScriptPool>() {
            private static final long serialVersionUID = 4717730055494117794L;

            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                boolean remove = size() > size;
                if (remove) {
                    evicted = eldest;
                } else {
                    evicted = null;
                }
                return remove;
            }
        };
    }

    public Object get(String name, LocalScriptExecutor executor, Script script) throws ScriptCompilationException {
        CompiledScriptPool compiledScriptPool;
        synchronized (mutex) {
            compiledScriptPool = cache.get(name);
        }
        if (compiledScriptPool != null) {
            return compiledScriptPool.get();
        }
        compiledScriptPool = compiledScriptPoolFactory.create();
        compiledScriptPool.init(executor, script);
        synchronized (mutex) {
            internalPut(name, compiledScriptPool);
        }
        return compiledScriptPool.get();
    }

    public void put(String name, Object compiledScript, LocalScriptExecutor executor) {
        synchronized (mutex) {
            CompiledScriptPool compiledScriptPool = cache.get(name);
            compiledScriptPool.put(compiledScript);
            internalPut(name, compiledScriptPool);
        }
    }

    private void internalPut(String name, CompiledScriptPool compiledScriptPool) {
        evicted = null;
        cache.put(name, compiledScriptPool);
        if (evicted != null) {
            ((CompiledScriptPool) evicted.getValue()).close();
        }
    }
}