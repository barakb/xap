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
import org.openspaces.remoting.scripting.ScriptingException;

/**
 * Represnts a pool of compiled scripts that should work with a non thread safe compiled scripts in
 * conjuction with {@link org.openspaces.remoting.scripting.cache.CompiledScriptCache}.
 *
 * @author kimchy
 */
public interface CompiledScriptPool {

    /**
     * Inits the pool by compiling zero or more scripts. This is benefitial when we have a pool and
     * we wish to initialize it with a pre set of compiled scripts.
     *
     * <p>Also, the local executor and the script can be store for later compilatation and
     * shutdowm.
     *
     * @param executor The local script executor to compile the script and close it.
     * @param script   The script to compile.
     */
    void init(LocalScriptExecutor executor, Script script) throws ScriptingException;

    /**
     * Returns a compiled script from the pool.
     */
    Object get() throws ScriptingException;

    /**
     * Puts back a compiled script to the pool.
     */
    void put(Object compiledScript) throws ScriptingException;

    /**
     * Closes the pool. Note, compiled scripts in the pool should be closed.
     */
    void close();
}
