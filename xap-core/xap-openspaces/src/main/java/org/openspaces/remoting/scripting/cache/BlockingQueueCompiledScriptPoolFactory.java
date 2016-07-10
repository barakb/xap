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

/**
 * A compiles script pool factory that create a {@link org.openspaces.remoting.scripting.cache.BlockingQueueCompiledScriptPool}.
 *
 * <p>Default size of the pool is 5. Meaning the compiled script pool will hold 5 compiled scripts
 * of the same script.
 *
 * @author kimchy
 */
public class BlockingQueueCompiledScriptPoolFactory implements CompiledScriptPoolFactory {

    public static final int DEFAULT_SIZE = 5;

    private int size = DEFAULT_SIZE;

    /**
     * Constructs a new blocking queue compiled script pool with default size of <code>5</code>.
     */
    public BlockingQueueCompiledScriptPoolFactory() {
        this(DEFAULT_SIZE);
    }

    /**
     * Constructs a new blocking queue compiled script pool with its size as a parameter.
     */
    public BlockingQueueCompiledScriptPoolFactory(int size) {
        this.size = size;
    }

    /**
     * Constructs a new {@link org.openspaces.remoting.scripting.cache.BlockingQueueCompiledScriptPool}
     * with the configured size.
     */
    public CompiledScriptPool create() {
        return new BlockingQueueCompiledScriptPool(size);
    }
}