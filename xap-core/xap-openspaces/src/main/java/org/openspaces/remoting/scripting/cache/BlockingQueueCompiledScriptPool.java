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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.remoting.scripting.LocalScriptExecutor;
import org.openspaces.remoting.scripting.Script;
import org.openspaces.remoting.scripting.ScriptingException;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A {@link java.util.concurrent.BlockingQueue} script pool with a configurable size.
 *
 * <p>Upon initialization the pool compiles "size" scripts and puts it in the queue.
 *
 * @author kimchy
 */
public class BlockingQueueCompiledScriptPool implements CompiledScriptPool {

    private static final Log logger = LogFactory.getLog(BlockingQueueCompiledScriptPool.class);

    private int size;

    private BlockingQueue<Object> queue;

    private LocalScriptExecutor executor;

    public BlockingQueueCompiledScriptPool(int size) {
        this.size = size;
    }

    public void init(LocalScriptExecutor executor, Script script) throws ScriptingException {
        this.executor = executor;
        queue = new ArrayBlockingQueue<Object>(size);
        for (int i = 0; i < size; i++) {
            queue.add(executor.compile(script));
        }
    }

    public Object get() throws ScriptingException {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new ScriptingException("Interrupted waiting for compiled script pool");
        }
    }

    public void put(Object compiledScript) throws ScriptingException {
        queue.offer(compiledScript);
    }

    public void close() {
        ArrayList<Object> compiledScripts = new ArrayList<Object>();
        queue.drainTo(compiledScripts);
        for (Object compiledScript : compiledScripts) {
            try {
                executor.close(compiledScript);
            } catch (Exception e) {
                logger.debug("Failed to close script [" + compiledScript + "], ignoring", e);
            }
        }
    }
}
