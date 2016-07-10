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
 * An exception handler to handle exception when invoking event listeners. The exception can be
 * propagated or handled internally.
 *
 * <p>If the event container is transactional, then propagating the exception will cause the
 * transaction to rollback, which handling it will cause the transaction to commit.
 *
 * @author kimchy (shay.banon)
 */
public interface EventExceptionHandler<T> {

    /**
     * A callback when a successful execution of a listener.
     *
     * @param data      The actual data object of the event
     * @param gigaSpace A GigaSpace instance that can be used to perform additional operations
     *                  against the space
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    Optional additional data or the actual source event data object (where
     *                  relevant)
     */
    void onSuccess(T data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source) throws RuntimeException;

    /**
     * A callback to handle exception in an event container. The handler can either handle the
     * exception or propagate it.
     *
     * <p>If the event container is transactional, then propagating the exception will cause the
     * transaction to rollback, while handling it will cause the transaction to commit.
     *
     * <p>The {@link org.springframework.transaction.TransactionStatus} can also be used to control
     * if the transaction should be rolled back without throwing an exception.
     *
     * @param exception The listener thrown exception
     * @param data      The actual data object of the event
     * @param gigaSpace A GigaSpace instance that can be used to perform additional operations
     *                  against the space
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    Optional additional data or the actual source event data object (where
     *                  relevant)
     */
    void onException(ListenerExecutionFailedException exception, T data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source) throws RuntimeException;
}
