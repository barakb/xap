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


package org.openspaces.core.map;

import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.map.IMap;
import com.j_spaces.map.MapEntryFactory;
import com.j_spaces.map.SpaceMapEntry;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.SpaceTimeoutException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The lock manager is built on top of {@link IMap} and supports the ability to lock and unlock
 * certain keys within the map.
 *
 * @author kimchy
 */
public class LockManager {

    /**
     * A empty lock value written to indicate a lock when there is no value to lock on (i.e. calling
     * lock on a key where there is no value in the cache).
     */
    final public static Object EMPTY_LOCK_VALUE = "";

    public static boolean isEmptyLockValue(Object value) {
        return (value instanceof String) && ((String) value).length() == 0;
    }

    private static Log logger = LogFactory.getLog(LockManager.class);

    private final IMap map;

    private final ConcurrentHashMap<String, Transaction> lockedUIDHashMap = new ConcurrentHashMap<String, Transaction>();

    private final IJSpace masterSpace;

    private final BlockingQueue<SpaceMapEntry> templatePool;

    private final DistributedTransactionManagerProvider transactionManagerProvider;

    /**
     * Creates a new Lock Manager based on the {@link com.j_spaces.map.IMap}.
     */
    public LockManager(IMap map) {
        this.map = map;
        this.masterSpace = map.getMasterSpace();
        try {
            transactionManagerProvider = new DistributedTransactionManagerProvider();
        } catch (TransactionException e) {
            throw new CannotCreateTransactionException("Failed to obtain transaction lock manager", e);
        }

        templatePool = new ArrayBlockingQueue<SpaceMapEntry>(1000);
        for (int i = 0; i < 1000; i++) {
            templatePool.add(MapEntryFactory.create());
        }
    }

    /**
     * Locks the given key for any updates. Retruns a {@link org.openspaces.core.map.LockHandle}
     * that can be used to perform specific updates under the same lock (by using the transaction
     * object stored within it).
     *
     * <p>Might create an empty value if there is no value in order to lock on. The empty value can
     * be checked using {@link #isEmptyLockValue(Object)}.
     *
     * @param key                   The key to lock
     * @param lockTimeToLive        The lock time to live (in milliseconds)
     * @param timeoutWaitingForLock The time to wait for an already locked lock
     * @return LockHandle that can be used to perfrom operations under the given lock
     */
    public LockHandle lock(Object key, long lockTimeToLive, long timeoutWaitingForLock) {

        String uid = String.valueOf(key);
        Transaction tr = null;
        try {
            tr = getTransaction(lockTimeToLive);
        } finally {
            if (tr == null) {
                lockedUIDHashMap.remove(uid);
                return null;
            }
        }

        SpaceMapEntry ee = getTemplate(key);
        try {
            Object retTake = masterSpace.readIfExists(ee, tr, timeoutWaitingForLock, ReadModifiers.EXCLUSIVE_READ_LOCK);
            if (retTake == null) {
                // TODO GS-9310: design and implement a solution for locking non-existent keys.
                map.put(key, EMPTY_LOCK_VALUE, tr, Integer.MAX_VALUE);
            }
        } catch (SpaceTimeoutException e) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", e);
            }
            // rethrow
            throw e;
        } catch (Throwable t) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", t);
            }
            lockedUIDHashMap.remove(uid);
            throw new DataAccessResourceFailureException("Failed to obtain lock for key [" + key + "]", t);
        } finally {
            releaseTemplate(ee);
        }

        //otherwise, map uid->txn
        lockedUIDHashMap.put(uid, tr);
        return new LockHandle(this, tr, key);
    }

    /**
     * Unlocks the given key and puts the given value in a single operation.
     *
     * @param key   The key to unlock and put the value in
     * @param value The value to put after unlocking the key
     */
    public void putAndUnlock(Object key, Object value) {
        String uid = String.valueOf(key);
        Transaction tr = lockedUIDHashMap.get(uid);

        if (tr == null) {
            map.put(key, value, null, Integer.MAX_VALUE);
            return;
        }

        try {
            map.put(key, value, tr, Integer.MAX_VALUE);
            tr.commit();
        } catch (Throwable t) {
            logger.warn("Failed to commit transaction and unlock the key [" + key + "], ignoring", t);
        } finally {
            lockedUIDHashMap.remove(uid);
        }
    }

    /**
     * Returns <code>true</code> if the given key is locked. Otherwise returns <code>false</code>.
     *
     * @param key The key to check if it locked or not.
     * @return <code>true</code> if the given key is locked or not.
     */
    public boolean islocked(Object key) {
        // first check locally
        String uid = String.valueOf(key);
        if (lockedUIDHashMap.containsKey(uid))
            return true;

        // now check globally
        SpaceMapEntry ee = getTemplate(key);
        try {
            Object lockEntry = masterSpace.readIfExists(ee, null, 0);
            return lockEntry == null;
        } catch (Exception e) {
            return true;
        } finally {
            releaseTemplate(ee);
        }
    }

    /**
     * Unlocks the given lock on the key
     *
     * @param key The key to unlock
     */
    public void unlock(Object key) {
        String uid = String.valueOf(key);

        Transaction tr = lockedUIDHashMap.get(uid);
        if (tr == null) {
            return;
        }

        try {
            tr.commit();
        } catch (Exception e) {
            logger.warn("Failed to commit transaction and unlocking the object, ignoring", e);
        } finally {
            lockedUIDHashMap.remove(uid);
        }
    }

    private Transaction getTransaction(long timeout) throws CannotCreateTransactionException {
        Transaction.Created tCreated;
        try {
            tCreated = TransactionFactory.create(transactionManagerProvider.getTransactionManager(), timeout);
        } catch (Exception e) {
            throw new CannotCreateTransactionException("Failed to create lock transaction", e);
        }
        return tCreated.transaction;
    }

    private SpaceMapEntry getTemplate(Object key) {
        SpaceMapEntry ee;
        try {
            ee = templatePool.poll(100, TimeUnit.MILLISECONDS);
            if (ee == null) {
                ee = MapEntryFactory.create();
            }
        } catch (InterruptedException e) {
            throw new DataAccessResourceFailureException("Failed to take resource from pool", e);
        }
        ee.setKey(key);
        // to support load balancing
        return ee;
    }

    private void releaseTemplate(SpaceMapEntry ee) {
        if (ee != null) {
            templatePool.offer(ee);
        }
    }

}
