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
import com.gigaspaces.async.internal.DefaultAsyncResult;

import net.jini.core.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.transaction.DefaultTransactionProvider;
import org.openspaces.core.transaction.manager.AbstractJiniTransactionManager;
import org.openspaces.core.transaction.manager.ExistingJiniTransactionManager;
import org.openspaces.core.transaction.manager.JiniTransactionHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @author kimchy
 */
public class InternalAsyncFutureListener<T> implements AsyncFutureListener<T> {

    private static final Log logger = LogFactory.getLog(InternalAsyncFutureListener.class);

    public static <T> AsyncFutureListener<T> wrapIfNeeded(AsyncFutureListener<T> listener, GigaSpace gigaSpace) {
        DefaultTransactionProvider txProvider = (DefaultTransactionProvider) gigaSpace.getTxProvider();
        JiniTransactionHolder holder = txProvider.getHolder();
        PlatformTransactionManager transactionManager = txProvider.getTransactionManager();
        if (holder == null || transactionManager == null) {
            // just wrap for exception translation
            return new InternalAsyncFutureListener<T>(gigaSpace, listener, null, transactionManager, holder);
        }
        // here, we create a dummy transaction status (with its new transaction set to true, so the commit/roolback
        // process will be performed). We also increase the ref count of the transaction, so only the last one will
        // be performed
        AbstractJiniTransactionManager.JiniTransactionObject jiniTransactionObject = new AbstractJiniTransactionManager.JiniTransactionObject();
        jiniTransactionObject.setJiniHolder(holder, false);
        TransactionStatus txStatus = new DefaultTransactionStatus(jiniTransactionObject, true, false, false, false, null);
        holder.incRef();
        return new InternalAsyncFutureListener<T>(gigaSpace, listener, txStatus, transactionManager, holder);
    }

    private final GigaSpace gigaSpace;

    private final AsyncFutureListener<T> listener;

    private final PlatformTransactionManager transactionManager;

    private final TransactionStatus txStatus;

    private final JiniTransactionHolder holder;

    private final boolean transactionalListener;

    public InternalAsyncFutureListener(GigaSpace gigaSpace, AsyncFutureListener<T> listener) {
        this(gigaSpace, listener, null, null, null);
    }

    public InternalAsyncFutureListener(GigaSpace gigaSpace, AsyncFutureListener<T> listener, TransactionStatus txStatus,
                                       PlatformTransactionManager transactionManager, JiniTransactionHolder holder) {
        this.gigaSpace = gigaSpace;
        this.listener = listener;
        this.txStatus = txStatus;
        this.transactionManager = transactionManager;
        this.holder = holder;
        this.transactionalListener = listener instanceof TransactionalAsyncFutureListener;
    }

    private static Exception translate(Exception exception, GigaSpace gigaSpace) {
        if (exception == null)
            return null;
        Exception translatedException = gigaSpace.getExceptionTranslator().translateNoUncategorized(exception);
        return translatedException != null ? translatedException : exception;
    }

    public void onResult(AsyncResult<T> asyncResult) {
        AsyncResult<T> result = new DefaultAsyncResult<T>(asyncResult.getResult(), translate(asyncResult.getException(), gigaSpace));
        Transaction tx = null;
        if (holder != null) {
            tx = holder.getTransaction();
        }
        if (tx != null) {
            try {
                ExistingJiniTransactionManager.bindExistingTransaction(tx, true, true);
                listener.onResult(result);
                if (transactionalListener) {
                    ((TransactionalAsyncFutureListener<T>) listener).onTransactionalResult(result, txStatus);
                }
                transactionManager.commit(txStatus);
                if (transactionalListener) {
                    ((TransactionalAsyncFutureListener<T>) listener).onPostCommitTransaction(result);
                }
            } catch (RuntimeException e) {
                try {
                    transactionManager.rollback(txStatus);
                } catch (RuntimeException e1) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to rollback transaction after failed commit", e1);
                    }
                }
                if (transactionalListener) {
                    ((TransactionalAsyncFutureListener<T>) listener).onPostRollbackTransaction(result);
                }
                throw e;
            } finally {
                ExistingJiniTransactionManager.unbindExistingTransactionIfPossible();
            }
        } else {
            listener.onResult(result);
            if (transactionalListener) {
                try {
                    ((TransactionalAsyncFutureListener<T>) listener).onTransactionalResult(result, txStatus);
                    ((TransactionalAsyncFutureListener<T>) listener).onPostCommitTransaction(result);
                } catch (RuntimeException e) {
                    ((TransactionalAsyncFutureListener<T>) listener).onPostRollbackTransaction(result);
                    throw e;
                }
            }
        }
    }
}
