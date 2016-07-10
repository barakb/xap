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
 * An optional filter that can be used with distributed tasks. Allows to be notified for each {@link
 * com.gigaspaces.async.AsyncResult} that arrives and decide what should be done with it.
 *
 * @author Assaf Ronen
 * @since 6.6
 */
public interface AsyncResultFilter<T> {
    /**
     * Controls what should be done with the results.
     */
    enum Decision {
        /**
         * Continue processing the distributed task.
         */
        CONTINUE,

        /**
         * Break out of the processing of the distributed task and move to the reduce phase
         * including the current result.
         */
        BREAK,

        /**
         * Skip this result and continue processing the rest of the results.
         */
        SKIP,

        /**
         * Skip this result and breaks out of the processing of the distributed task and move to the
         * reduce phase.
         */
        SKIP_AND_BREAK
    }

    /**
     * A callback invoked for each result that arrives as a result of a distributed task execution
     * allowing to access the result that caused this event, the events received so far, and the
     * total expected results.
     */
    Decision onResult(AsyncResultFilterEvent<T> event);
}
