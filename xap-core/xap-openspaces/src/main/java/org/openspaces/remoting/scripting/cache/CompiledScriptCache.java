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

/**
 * A compiled script cache. Allows to get a compiled script from the cache, and out it back.
 *
 * @author kimchy
 */
public interface CompiledScriptCache {

    /**
     * Gets the compiled script from the cache. Compiles it if a new instance is needed using the
     * provided local executor.
     *
     * @param name     The name (key) in the cache.
     * @param executor The executor to use to compile the script.
     * @param script   The script to compile
     * @return A compiled script from the cache
     */
    Object get(String name, LocalScriptExecutor executor, Script script) throws ScriptCompilationException;

    /**
     * Puts a compiled script back to the cache.
     *
     * @param name           The name (key) in the cache)
     * @param compiledScript The compiled script to put back in the cache.
     * @param executor       The executor to use in order to close any evicted compiled scripts from
     *                       the cache
     */
    void put(String name, Object compiledScript, LocalScriptExecutor executor);
}
