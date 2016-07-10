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

package org.openspaces.core;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface PollingFuture<T> {
    /**
     * @return the result, or one of the following exceptions
     * @throws ExecutionException    - execution failed and an exception was raised during work
     * @throws IllegalStateException - execution not done yet. Call this method only when @{link
     *                               {@link #isDone()} returns true.
     * @throws TimeoutException      - execution timed out and result is not available.
     */
    T get() throws ExecutionException, IllegalStateException, TimeoutException;

    /**
     * @return true if execution completed successfully, with an exception or timed-out. If it is
     * still in progress returns null.
     */
    boolean isDone();

    /**
     * @return true if execution timed out.
     */
    boolean isTimedOut();

    /**
     * @return the execption raised during execution wraped as an ExecutionException or null if in
     * progress or no exception was raised.
     */
    ExecutionException getException();

    Date getTimestamp();
}
