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
 * An LRU cache that can handle compiled scripts that are thread safe. Thread safe compiled scripts
 * are scripts that can be executed by several threads at the same time.
 *
 * <p>Uses a simple {@link java.util.LinkedHashMap} to implement the LRU.
 *
 * @author kimchy
 */
public class LRUThreadSafeCompiledScriptCache implements CompiledScriptCache {

    public static final int DEFAULT_CACHE_SIZE = 50;

    private final int size;

    private final LinkedHashMap<String, Object> cache;

    private final Object mutex = new Object();

    private Map.Entry evicted;

    public LRUThreadSafeCompiledScriptCache() {
        this(DEFAULT_CACHE_SIZE);
    }

    public LRUThreadSafeCompiledScriptCache(int cacheSize) {
        this.size = cacheSize;
        cache = new LinkedHashMap<String, Object>() {
            private static final long serialVersionUID = 3509768785591597933L;

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
        Object compiledScript;
        synchronized (mutex) {
            compiledScript = cache.get(name);
            if (compiledScript != null) {
                return compiledScript;
            }
        }
        return executor.compile(script);
    }

    public void put(String name, Object compiledScript, LocalScriptExecutor executor) {
        synchronized (mutex) {
            evicted = null;
            cache.put(name, compiledScript);
            if (evicted != null) {
                executor.close(evicted.getValue());
            }
        }
    }
}
