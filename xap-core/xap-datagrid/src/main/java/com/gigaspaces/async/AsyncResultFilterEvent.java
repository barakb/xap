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


package com.gigaspaces.async;

/**
 * Represents an {@link com.gigaspaces.async.AsyncResultFilter} event.
 *
 * @author kimchy
 */

public class AsyncResultFilterEvent<T> {
    private final AsyncResult<T> currentResult;
    private final AsyncResult<T>[] receivedResults;
    private final int totalExpectedResults;

    /**
     * Constructs a new async result filter event
     *
     * @param currentResult        The current result
     * @param receivedResults      The received results so far without the current result
     * @param totalExpectedResults The total results that are expected of this execution
     */
    public AsyncResultFilterEvent(AsyncResult<T> currentResult, AsyncResult<T>[] receivedResults, int totalExpectedResults) {
        this.currentResult = currentResult;
        this.receivedResults = receivedResults;
        this.totalExpectedResults = totalExpectedResults;
    }

    /**
     * Returns the current result that caused this event.
     */
    public AsyncResult<T> getCurrentResult() {
        return currentResult;
    }

    /**
     * Returns the received results without the current result.
     */
    public AsyncResult<T>[] getReceivedResults() {
        return receivedResults;
    }

    /**
     * Returns the total expected results of this execution.
     */
    public int getTotalExpectedResults() {
        return totalExpectedResults;
    }
}
