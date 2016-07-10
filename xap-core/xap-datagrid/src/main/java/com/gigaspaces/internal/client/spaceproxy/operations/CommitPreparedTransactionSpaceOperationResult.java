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

package com.gigaspaces.internal.client.spaceproxy.operations;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author Yechiel
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class CommitPreparedTransactionSpaceOperationResult extends SpaceOperationResult {
    private static final long serialVersionUID = 1L;

    /**
     * Required for Externalizable
     */
    public CommitPreparedTransactionSpaceOperationResult() {
    }

    public void processExecutionException() throws TransactionException, RemoteException, InterruptedException {
        final Exception executionException = getExecutionException();
        if (executionException == null)
            return;
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof InterruptedException)
            throw (InterruptedException) executionException;

        onUnexpectedException(executionException);
    }
}
