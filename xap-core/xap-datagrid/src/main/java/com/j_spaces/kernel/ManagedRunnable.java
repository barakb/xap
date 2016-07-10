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

package com.j_spaces.kernel;

/**
 * A Managed Runnable is a convenient superclass for Runnables that want to support graceful
 * shutdown of threads. When someone wants the thread to be shut down, it calls requestShutdown().
 * The thread periodically checks if it should shut down by calling shouldShutdown(), and returns
 * from run() if it should.
 */
abstract public class ManagedRunnable {
    private volatile boolean m_Shutdown;

    /**
     * Request shutdown, this method will be block depends the waitWhileFinish() method
     * implementation.
     **/
    public void requestShutdown() {
        m_Shutdown = true;

        /** wait while shutdown */
        waitWhileFinish();
    }

    public boolean shouldShutdown() {
        return m_Shutdown || Thread.currentThread().isInterrupted();
    }

    /**
     * This method will be invoked by requestShutdown(). Request method will be block while
     * waitWhileFinish() will not be finished.
     **/
    protected abstract void waitWhileFinish();
}