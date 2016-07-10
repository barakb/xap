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


package org.openspaces.core.transaction.manager;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.LeaseException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.NestableTransactionManager;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.lease.LeaseRenewalManager;

import org.openspaces.pu.service.ServiceDetailsProvider;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.NestedTransactionNotSupportedException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.rmi.RemoteException;

/**
 * Base class for Jini implementation of Springs {@link PlatformTransactionManager}. Uses Jini
 * {@link TransactionManager} in order to manage transactions with sub classes responsible for
 * providing it using {@link #doCreateTransactionManager()}.
 *
 * <p>Jini transactions are bounded under the {@link #setTransactionalContext(Object)} using Springs
 * {@link TransactionSynchronizationManager#bindResource(Object, Object)}. The transactional context
 * is optional and defaults to the Jini {@link TransactionManager} instance. Note, this can be
 * overridden by sub classes.
 *
 * <p>By default the transaction timeout will be <code>90</code> seconds. The default timeout on the
 * transaction manager level can be set using {@link #setDefaultTimeout(int)}. If the timeout is
 * explicitly set using Spring support for transactions (for example using {@link
 * org.springframework.transaction.TransactionDefinition}) this value will be used.
 *
 * @author kimchy
 * @see org.openspaces.core.transaction.DefaultTransactionProvider
 * @see org.openspaces.core.transaction.manager.JiniTransactionHolder
 */
public abstract class AbstractJiniTransactionManager extends AbstractPlatformTransactionManager implements
        JiniPlatformTransactionManager, InitializingBean, BeanNameAware, ServiceDetailsProvider, DisposableBean {

    private static final long serialVersionUID = 4217156441204875733L;

    protected static final String SERVICE_TYPE = "tx-manager";

    static final long DEFAULT_TX_TIMEOUT = 90000L;
    static final long DEFAULT_TX_COMMIT_TIMEOUT = Lease.FOREVER;
    static final long DEFAULT_TX_ROLLBACK_TIMEOUT = Lease.FOREVER;


    // TransactionManager used for creating the actual transaction
    private transient TransactionManager transactionManager;

    // the jini participant - can be javaspace or any other service that wants
    // to take part in the transaction
    private Object transactionalContext;

    private Long commitTimeout = DEFAULT_TX_COMMIT_TIMEOUT;

    private Long rollbackTimeout = DEFAULT_TX_ROLLBACK_TIMEOUT;

    private TransactionLeaseRenewalConfig leaseRenewalConfig;

    private LeaseRenewalManager[] leaseRenewalManagers;

    protected String beanName;


    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public String getBeanName() {
        return this.beanName;
    }

    public Object getTransactionalContext() {
        return transactionalContext;
    }

    public void setTransactionalContext(Object txResource) {
        this.transactionalContext = txResource;
    }

    /**
     * Sets an optional timeout when performing commit in milliseconds. Default: {@link
     * org.openspaces.core.transaction.manager.AbstractJiniTransactionManager#DEFAULT_TX_COMMIT_TIMEOUT}.
     */
    public void setCommitTimeout(Long commitTimeout) {
        this.commitTimeout = commitTimeout;
    }

    /**
     * Sets an optional timeout when performing rollback/abort in milliseconds. Default: {@link
     * org.openspaces.core.transaction.manager.AbstractJiniTransactionManager#DEFAULT_TX_ROLLBACK_TIMEOUT}.
     */
    public void setRollbackTimeout(Long rollbackTimeout) {
        this.rollbackTimeout = rollbackTimeout;
    }

    /**
     * Sets the transaction lease renewal configuration. Once set, transactions will be renewed
     * automatically. If not set, no renewals will occur.
     */
    public void setLeaseRenewalConfig(TransactionLeaseRenewalConfig leaseRenewalConfig) {
        this.leaseRenewalConfig = leaseRenewalConfig;
    }

    /**
     * Implemented by sub classes to provide a Jini {@link TransactionManager}.
     */
    protected abstract TransactionManager doCreateTransactionManager() throws Exception;

    public TransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    public void afterPropertiesSet() throws Exception {
        this.transactionManager = doCreateTransactionManager();
        Assert.notNull(this.transactionManager, "Jini transactionManager is required");
        if (transactionalContext == null) {
            transactionalContext = transactionManager;
        }
        if ((transactionManager instanceof NestableTransactionManager)) {
            setNestedTransactionAllowed(true);
        }
        if (leaseRenewalConfig != null) {
            leaseRenewalManagers = new LeaseRenewalManager[leaseRenewalConfig.getPoolSize()];
            for (int i = 0; i < leaseRenewalConfig.getPoolSize(); i++)
                leaseRenewalManagers[i] = new LeaseRenewalManager(leaseRenewalConfig.getRenewRTT(), 2);
            logger.debug(logMessage("Created transaction manager with lease renewal pool [" + leaseRenewalConfig.getPoolSize() + "] and RTT [" + leaseRenewalConfig.getRenewRTT() + "]"));
        }
    }

    public void destroy() throws Exception {
        if (leaseRenewalManagers != null) {
            for (LeaseRenewalManager leaseRenewalManager : leaseRenewalManagers) {
                leaseRenewalManager.terminate();
            }
            leaseRenewalManagers = null;
        }
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {

        JiniTransactionObject txObject = new JiniTransactionObject();
        // txObject.setNestedTransactionAllowed
        // txObject.setJiniHolder(transactionalContext);

        // set the jini holder is one is found
        JiniTransactionHolder jiniHolder = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(transactionalContext);
        if (jiniHolder == null) {
            jiniHolder = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(ExistingJiniTransactionManager.CONTEXT);
        }
        if (jiniHolder != null) {
            if (logger.isTraceEnabled()) {
                logger.trace(logMessage("Found thread-bound tx data [" + jiniHolder + "] for Jini resource ["
                        + transactionalContext + "]"));
            }
            txObject.setJiniHolder(jiniHolder, false);
        }

        return txObject;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) transaction;
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Beginning transaction [" + txObject + "]"));
        try {
            doJiniBegin(txObject, definition);
        } catch (UnsupportedOperationException ex) {
            // assume nested transaction not supported
            throw new NestedTransactionNotSupportedException("Implementation does not ex nested transactions", ex);
        }
    }

    protected void doJiniBegin(JiniTransactionObject txObject, TransactionDefinition definition) {

        // create the tx

        try {
            if (txObject.getJiniHolder() == null) {
                long timeout = DEFAULT_TX_TIMEOUT;
                if (getDefaultTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                    timeout = getDefaultTimeout() * 1000L;
                }
                if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                    timeout = definition.getTimeout() * 1000L;
                }
                if (logger.isTraceEnabled()) {
                    logger.trace(logMessage("Creating new transaction for [" + transactionalContext + "] with timeout [" + timeout + " milliseconds]"));
                }
                Transaction.Created txCreated;
                LeaseRenewalManager leaseRenewalManager = null;
                if (leaseRenewalConfig != null) {
                    // if we are working under a lease renewal manager, create a transaction with the duration
                    // and create a lease renewal manager that will renew the lease each duration till the
                    // configured timeout
                    txCreated = TransactionFactory.create(transactionManager, leaseRenewalConfig.getRenewDuration());
                    leaseRenewalManager = leaseRenewalManagers[Math.abs(txCreated.hashCode() % leaseRenewalConfig.getPoolSize())];
                    leaseRenewalManager.renewFor(txCreated.lease, timeout,
                            leaseRenewalConfig.getRenewDuration(), leaseRenewalConfig.getLeaseListener());
                } else {
                    txCreated = TransactionFactory.create(transactionManager, timeout);
                }

                JiniTransactionHolder jiniHolder = new JiniTransactionHolder(txCreated, definition.getIsolationLevel(), leaseRenewalManager);
                jiniHolder.setTimeoutInSeconds(definition.getTimeout());
                txObject.setJiniHolder(jiniHolder, true);
            }

            txObject.getJiniHolder().setSynchronizedWithTransaction(true);

            applyIsolationLevel(txObject, definition.getIsolationLevel());

            // Bind the session holder to the thread.
            if (txObject.isNewJiniHolder()) {
                TransactionSynchronizationManager.bindResource(transactionalContext, txObject.getJiniHolder());
            }
        } catch (LeaseDeniedException e) {
            throw new CannotCreateTransactionException("Lease denied", e);
        } catch (RemoteException e) {
            throw new CannotCreateTransactionException("Remote exception", e);
        }

    }

    protected void applyIsolationLevel(JiniTransactionObject txObject, int isolationLevel)
            throws InvalidIsolationLevelException {
        if (isolationLevel != TransactionDefinition.ISOLATION_DEFAULT) {
            if (!suppportsCustomIsolationLevel())
                throw new InvalidIsolationLevelException(getClass().getName() + " does not support custom isolation levels");
            if (isolationLevel == TransactionDefinition.ISOLATION_SERIALIZABLE)
                throw new InvalidIsolationLevelException("Jini Transaction Manager does not support serializable isolation level");
        }
    }

    protected boolean suppportsCustomIsolationLevel() {
        return false;
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) status.getTransaction();
        if (txObject.getJiniHolder().isDisableCommit()) {
            if (logger.isTraceEnabled())
                logger.trace(logMessage("Disabling commit on Jini transaction [" + txObject.toString() + "]"));
            return;
        }
        if (txObject.getJiniHolder().decRef() > 0) {
            if (logger.isTraceEnabled())
                logger.trace(logMessage("Disabling commit on Jini transaction reference count is higher [" + txObject.toString() + "]"));
            return;
        }
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Committing Jini transaction [" + txObject.toString() + "]"));
        try {
            if (commitTimeout == null) {
                txObject.getTransaction().commit();
            } else {
                txObject.getTransaction().commit(commitTimeout);
            }
        } catch (UnknownTransactionException e) {
            throw convertJiniException(e);
        } catch (CannotCommitException e) {
            throw convertJiniException(e);
        } catch (RemoteException e) {
            throw convertJiniException(e);
        } catch (TimeoutExpiredException e) {
            throw convertJiniException(e);
        }
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) transaction;
        return txObject.hasTransaction();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) status.getTransaction();
        if (txObject.getJiniHolder().isDisableRollback()) {
            if (logger.isTraceEnabled())
                logger.trace(logMessage("Disabling rollback on Jini transaction [" + txObject.toString() + "]"));
            return;
        }
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Rolling back Jini transaction [" + txObject + "]"));
        try {
            if (rollbackTimeout == null) {
                txObject.getTransaction().abort();
            } else {
                txObject.getTransaction().abort(rollbackTimeout);
            }
        } catch (UnknownTransactionException e) {
            throw convertJiniException(e);
        } catch (CannotAbortException e) {
            throw convertJiniException(e);
        } catch (RemoteException e) {
            throw convertJiniException(e);
        } catch (TimeoutExpiredException e) {
            throw convertJiniException(e);
        } finally {
            // disable commit / rollback so other commits/rollbacks will not take affect
            txObject.getJiniHolder().setDisableCommit(true);
            txObject.getJiniHolder().setDisableRollback(true);
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        JiniTransactionObject txObject = (JiniTransactionObject) transaction;
        // Remove the session holder from the thread.
        if (txObject.isNewJiniHolder()) {
            if (logger.isTraceEnabled()) {
                logger.trace(logMessage("Removing per-thread Jini transaction for [" + getTransactionalContext() + "]"));
            }
            TransactionSynchronizationManager.unbindResource(getTransactionalContext());

            // remove the lease from the lease renewal manager
            if (txObject.getJiniHolder().hasLeaseRenewalManager()) {
                try {
                    txObject.getJiniHolder().getLeaseRenewalManager().remove(txObject.getJiniHolder().getTxCreated().lease);
                } catch (UnknownLeaseException e) {
                    logger.debug(logMessage("Got an unknown lease exception for [" + txObject + "]"), e);
                }
            }
        }
        txObject.getJiniHolder().clear();
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug(logMessage("Setting Jini transaction on txContext [" + getTransactionalContext() + "] rollback-only"));
        }
        txObject.setRollbackOnly();
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources) throws TransactionException {
        if (suspendedResources instanceof ExisitingJiniTransactionHolder)
            ExistingJiniTransactionManager.bindExistingTransaction((ExisitingJiniTransactionHolder) suspendedResources);
        else
            TransactionSynchronizationManager.bindResource(getTransactionalContext(), (JiniTransactionHolder) suspendedResources);
    }

    @Override
    protected Object doSuspend(Object transaction) throws TransactionException {
        JiniTransactionObject txObject = (JiniTransactionObject) transaction;
        txObject.setJiniHolder(null, false);
        Object unbindResource = TransactionSynchronizationManager.unbindResourceIfPossible(getTransactionalContext());
        if (unbindResource == null) {
            unbindResource = TransactionSynchronizationManager.unbindResource(ExistingJiniTransactionManager.CONTEXT);
        }
        return unbindResource;
    }

    @Override
    protected boolean useSavepointForNestedTransaction() {
        return false;
    }

    protected RuntimeException convertJiniException(Exception exception) {
        if (exception instanceof LeaseException) {
            return new RemoteAccessException("Lease denied", exception);
        }

        if (exception instanceof TransactionException || exception instanceof net.jini.core.transaction.TransactionException) {
            return new TransactionSystemException(exception.getMessage(), exception);
        }

        if (exception instanceof RemoteException) {
            // Translate to Spring's unchecked remote access exception
            return new RemoteAccessException("RemoteException", exception);
        }

        if (exception instanceof UnusableEntryException) {
            return new RemoteAccessException("Unusable entry", exception);
        }

        if (exception instanceof RuntimeException) {
            return (RuntimeException) exception;
        }

        if (exception instanceof TimeoutExpiredException) {
            throw new TransactionTimedOutException("Transaction timed out (either the transaction or commit/abort)",
                    exception);
        }

        return new UnexpectedTransactionException(exception);
    }


    public static class UnexpectedTransactionException extends TransactionException {

        private static final long serialVersionUID = 1L;

        public UnexpectedTransactionException(Exception cause) {
            super("unexpected exception ", cause);
        }
    }

    /**
     * Jini Transaction object. Used as transaction object by GigaSpaceTransactionManager.
     */
    public static class JiniTransactionObject implements SmartTransactionObject {

        private JiniTransactionHolder jiniHolder;

        private boolean newJiniHolder;

        public boolean hasTransaction() {
            return (jiniHolder != null && jiniHolder.hasTransaction());
        }

        public void setJiniHolder(JiniTransactionHolder jiniHolder, boolean newSessionHolder) {
            this.jiniHolder = jiniHolder;
            this.newJiniHolder = newSessionHolder;
        }

        public JiniTransactionHolder getJiniHolder() {
            return jiniHolder;
        }

        public boolean isNewJiniHolder() {
            return newJiniHolder;
        }

        public boolean isRollbackOnly() {
            return (jiniHolder != null && jiniHolder.isRollbackOnly());
        }

        public void setRollbackOnly() {
            if (jiniHolder != null) {
                jiniHolder.setRollbackOnly();
            }
        }

        public void flush() {
            // nothing to do here
        }

        public Transaction getTransaction() {
            if (hasTransaction()) {
                return jiniHolder.getTxCreated().transaction;
            }
            return null;
        }

    }

    protected String logMessage(String message) {
        return "[" + beanName + "] " + message;
    }
}
