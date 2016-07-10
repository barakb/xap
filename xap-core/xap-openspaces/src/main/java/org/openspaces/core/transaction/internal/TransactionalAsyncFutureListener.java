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


package org.openspaces.core.transaction.internal;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;

import org.springframework.transaction.TransactionStatus;

/**
 * Extends the future listener, resulting in calling the {@link #onTransactionalResult(com.gigaspaces.async.AsyncResult,
 * org.springframework.transaction.TransactionStatus)} after the {@link
 * #onResult(com.gigaspaces.async.AsyncResult)}. If running within a transaction, will execute it
 * with null within the transaction status.
 *
 * @author kimchy
 */
public interface TransactionalAsyncFutureListener<T> extends AsyncFutureListener<T> {

    void onTransactionalResult(AsyncResult<T> asyncResult, TransactionStatus txStatus);

    void onPostCommitTransaction(AsyncResult<T> asyncResult);

    void onPostRollbackTransaction(AsyncResult<T> asyncResult);
}
