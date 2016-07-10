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


package org.openspaces.events;

import org.openspaces.core.GigaSpace;
import org.springframework.transaction.TransactionStatus;

/**
 * A Space data event listener interface allowing for reception of events triggered by different
 * container types. Note, changing the container types should be just a matter of configuration,
 * with the event handling code remaining the same. For simplified, Pojo like, event listeners see
 * the adapter package.
 *
 * @author kimchy
 * @see org.openspaces.events.adapter.MethodEventListenerAdapter
 * @see org.openspaces.events.adapter.AnnotationEventListenerAdapter
 */
public interface SpaceDataEventListener<T> {

    /**
     * An event callback with the actual data object of the event.
     *
     * @param data      The actual data object of the event
     * @param gigaSpace A GigaSpace instance that can be used to perform additional operations
     *                  against the space
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    Optional additional data or the actual source event data object (where
     *                  relevant)
     */
    void onEvent(T data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source);
}
