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


package org.openspaces.remoting.scripting;

import java.util.concurrent.Future;

/**
 * Scripting executor allows to execute {@link Script} using remoting. The "server" side implements
 * this interface (with built in implementation of {@link DefaultScriptingExecutor}. The client side
 * simply uses either async or sync proxy built on top of this interface.
 *
 * @author kimchy
 */
public interface ScriptingExecutor<T> {

    /**
     * Executes the given script and returns a response.
     */
    T execute(Script script) throws ScriptingException;

    /**
     * Executes the given script and return a future that can be used to read the response at a
     * later stage.
     *
     * <p>Note, this only works with "async" remoting.
     */
    Future<T> asyncExecute(Script script) throws ScriptingException;
}
