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

import org.openspaces.core.GigaSpace;
import org.openspaces.remoting.ExecutorRemotingProxyConfigurer;

/**
 * A simple programmatic configurer creating a remote executor scripting proxy.
 *
 * <p>Usage example:
 * <pre>
 * IJSpace space = new UrlSpaceConfigurer("jini://&#42;/&#42;/mySpace")
 *                        .space();
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(space).gigaSpace();
 * ScriptingExecutor<Integer> executor = new ExecutorScriptingProxyConfigurer<Integer>(gigaSpace)
 *                                       .timeout(15000)
 *                                       .scriptingExecutor();
 * Integer result = executor.execute(new StaticScript()
 *                  .type("groovy")
 *                  .name("myScript")
 *                  .script("return 1"));
 * </pre>
 *
 * @author Uri Cohen
 */
public class ExecutorScriptingProxyConfigurer<T> {

    private ExecutorRemotingProxyConfigurer<ScriptingExecutor> remotingConfigurer;

    /**
     * Creates a new <code>ExecutorScriptingProxyConfigurer</code> on top of the given space
     */
    public ExecutorScriptingProxyConfigurer(GigaSpace gigaSpace) {
        remotingConfigurer = new ExecutorRemotingProxyConfigurer<ScriptingExecutor>(gigaSpace, ScriptingExecutor.class)
                .metaArgumentsHandler(new ScriptingMetaArgumentsHandler())
                .remoteInvocationAspect(new LazyLoadingRemoteInvocationAspect())
                .remoteRoutingHandler(new ScriptingRemoteRoutingHandler());
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    public ExecutorScriptingProxyConfigurer<T> timeout(long timeout) {
        remotingConfigurer.timeout(timeout);
        return this;
    }

    /**
     * Create a new executor <code>ScriptingExecutor</code> proxy
     */
    public ScriptingExecutor<T> scriptingExecutor() {
        return remotingConfigurer.proxy();
    }
}