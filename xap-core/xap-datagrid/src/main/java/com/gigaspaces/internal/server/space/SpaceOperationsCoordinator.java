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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.utils.concurrent.SegmentedReentrantReadWriteLock;

/**
 * Coordinates execution of Concurrent and Critical space operations: - While a critical operation
 * is executing, any other incoming operation is blocked until the critical operation is finished. -
 * While a concurrent operation is executing, incoming concurrent operations are not blocked, but
 * critical operations are blocked until all running concurrent operations are finished. Requests
 * for concurrent operations which arrive after while there's a pending critical operations are
 * blocked and scheduled afterwards, to avoid starvation of critical operations.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceOperationsCoordinator {
    private final SegmentedReentrantReadWriteLock _readWriteLock;

    public SpaceOperationsCoordinator(int numOfSegments) {
        this._readWriteLock = numOfSegments < 1 ? null : new SegmentedReentrantReadWriteLock(numOfSegments, true);
    }

    /**
     * Begins a concurrent operation. If a exclusive operation is in progress (i.e. {@link
     * #beginExclusiveOperation()} was called but {@link #endExclusiveOperation()} was not called
     * yet), invocation is blocked until the exclusive operation is finished. Blocking management is
     * fair, i.e. if {@link #beginExclusiveOperation()} was invoked and blocked, subsequent calls to
     * {@link #beginConcurrentOperation()} will be blocked as well and will be scheduled after the
     * exclusive operation. It is the responsibility of the caller to call {@link
     * #endConcurrentOperation()} when the operation is finished.
     */
    public void beginConcurrentOperation() {
        if (_readWriteLock != null)
            _readWriteLock.acquireThreadReadLock();
    }

    /**
     * Ends a concurrent operation which was previously started via {@link
     * #beginConcurrentOperation()}.
     */
    public void endConcurrentOperation() {
        if (_readWriteLock != null)
            _readWriteLock.releaseThreadReadLock();
    }

    /**
     * Begins a exclusive operation. If one or more concurrent operations are in progress (i.e.
     * {@link #beginConcurrentOperation()} was called but {@link #endConcurrentOperation()} was not
     * called yet), invocation is blocked until all concurrent operations are finished. Blocking
     * management is fair, i.e. if {@link #beginExclusiveOperation()} was invoked and blocked,
     * subsequent calls to {@link #beginConcurrentOperation()} will be blocked as well and will be
     * scheduled after the exclusive operation. It is the responsibility of the caller to call
     * {@link #endConcurrentOperation()} when the operation is finished.
     */
    public void beginExclusiveOperation() {
        if (_readWriteLock != null)
            _readWriteLock.acquireWriteLock();
    }

    /**
     * Ends a exclusive operation which was previously started via {@link
     * #beginExclusiveOperation()}.
     */
    public void endExclusiveOperation() {
        if (_readWriteLock != null)
            _readWriteLock.releaseWriteLock();
    }
}
