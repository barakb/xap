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


package org.openspaces.events.notify;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables batching of notifications. Requires setting {@link #size()} and {@link #time()} which
 * control when the batched notifications will be sent to the listener.
 *
 * @author kimchy
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NotifyBatch {

    /**
     * The batch size controls the number of notifications that will be sent in each batch.
     */
    int size();

    /**
     * The batch time controls the elapsed time until the batch buffer is cleared and sent. The time
     * is in <b>milliseconds</b>.
     */
    int time();

    /**
     * The batch pending threshold controls the number of notifications that will be accumulated
     * before they are sent. If not set, size parameter will be used.
     */
    int pendingThreshold() default -1;

    /**
     * When batching is turned on, should the batch of events be passed as an <code>Object[]</code>
     * to the listener. Default to <code>false</code> which means it will be passed one event at a
     * time.
     */
    boolean passArrayAsIs() default false;

}
