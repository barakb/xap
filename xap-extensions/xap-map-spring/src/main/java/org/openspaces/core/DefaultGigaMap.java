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


package org.openspaces.core;

import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.javax.cache.CacheEntry;
import com.j_spaces.javax.cache.CacheException;
import com.j_spaces.javax.cache.CacheListener;
import com.j_spaces.map.IMap;

import net.jini.core.transaction.Transaction;

import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.map.LockHandle;
import org.openspaces.core.map.LockManager;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.transaction.TransactionDefinition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of the {@link GigaMap} interface simplifying the work with JCache and Map
 * interface on top of the Space.
 *
 * <p>Provides declarative transactions support (for methods that are by nature transactional, such
 * as <code>get</code> and <code>put</code>) and the ability to set defaults for both
 * <code>timeToLive</code> and <code>waitForResponse</code>.
 *
 * <p>Will also automatically apply the current running transaction isolation level when performing
 * read operations (that in turn are translated into the Space).
 *
 * @author kimchy
 */
public class DefaultGigaMap implements GigaMap {

    final private IMap map;

    final private TransactionProvider txProvider;

    final private ExceptionTranslator exTranslator;

    private long defaultWaitForResponse = 0;

    private long defaultTimeToLive = Long.MAX_VALUE;

    private long defaultLockTimeToLive = 60000;

    private long defaultWaitingForLockTimeout = 10000;

    private int defaultIsolationLevel;

    final private LockManager lockManager;

    /**
     * Constructs a new DefaultGigaMap implementation.
     *
     * @param map          The map implementation to delegate operations to
     * @param txProvider   The transaction provider for declarative transaction ex.
     * @param exTranslator Exception translator to translate low level exceptions into GigaSpaces
     *                     runtime exception
     */
    public DefaultGigaMap(IMap map, TransactionProvider txProvider, ExceptionTranslator exTranslator,
                          int defaultIsolationLevel) {
        this.map = map;
        this.lockManager = new LockManager(map);
        this.txProvider = txProvider;
        this.exTranslator = exTranslator;
        // set the default read take modifiers according to the default isolation level
        // NOTE: by default, Map implementation use REPEATABLE_READ
        switch (defaultIsolationLevel) {
            case TransactionDefinition.ISOLATION_DEFAULT:
                this.defaultIsolationLevel = ReadModifiers.REPEATABLE_READ;
                break;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
                this.defaultIsolationLevel = ReadModifiers.DIRTY_READ;
                break;
            case TransactionDefinition.ISOLATION_READ_COMMITTED:
                this.defaultIsolationLevel = ReadModifiers.READ_COMMITTED;
                break;
            case TransactionDefinition.ISOLATION_REPEATABLE_READ:
                this.defaultIsolationLevel = ReadModifiers.REPEATABLE_READ;
                break;
            case TransactionDefinition.ISOLATION_SERIALIZABLE:
                throw new IllegalArgumentException("GigaMap does not support serializable isolation level");
        }
    }

    public void setDefaultWaitForResponse(long defaultWaitForResponse) {
        this.defaultWaitForResponse = defaultWaitForResponse;
    }

    public void setDefaultTimeToLive(long defaultTimeToLive) {
        this.defaultTimeToLive = defaultTimeToLive;
    }

    public void setDefaultLockTimeToLive(long defaultLockTimeToLive) {
        this.defaultLockTimeToLive = defaultLockTimeToLive;
    }

    public void setDefaultWaitingForLockTimeout(long defaultWaitingForLockTimeout) {
        this.defaultWaitingForLockTimeout = defaultWaitingForLockTimeout;
    }

    public IMap getMap() {
        return this.map;
    }

    public TransactionProvider getTxProvider() {
        return this.txProvider;
    }

    public int size() {
        try {
            return map.size();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public boolean isEmpty() {
        try {
            return map.isEmpty();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public boolean containsKey(Object key) {
        try {
            return map.containsKey(key);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public boolean containsValue(Object value) {
        try {
            return map.containsValue(value);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object get(Object key) {
        return get(key, defaultWaitForResponse);
    }

    public Object get(Object key, long waitForResponse) {
        return get(key, waitForResponse, getModifiersForIsolationLevel());
    }

    public Object get(Object key, long waitForResponse, int modifiers) {
        try {
            return map.get(key, getCurrentTransaction(), waitForResponse, modifiers);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object put(Object key, Object value) {
        return put(key, value, defaultTimeToLive);
    }

    public Object put(Object key, Object value, long timeToLive) {
        return put(key, value, timeToLive, defaultWaitForResponse);
    }

    public Object put(Object key, Object value, long timeToLive, long timeout) {
        try {
            return map.put(key, value, getCurrentTransaction(), timeToLive, timeout);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object put(Object key, Object value, LockHandle lockHandle) {
        return put(key, value, defaultTimeToLive, lockHandle);
    }

    public Object put(Object key, Object value, long timeToLive, LockHandle lockHandle) {
        try {
            return map.put(key, value, lockHandle.getTransaction(), timeToLive);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object remove(Object key) {
        return remove(key, defaultWaitForResponse);
    }

    public Object remove(Object key, long waitForResponse) {
        try {
            return map.remove(key, getCurrentTransaction(), waitForResponse);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object remove(Object key, long waitForResponse, LockHandle lockHandle) {
        try {
            return map.remove(key, lockHandle.getTransaction(), waitForResponse);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void putAll(Map t) {
        putAll(t, defaultTimeToLive);
    }

    public void putAll(Map t, long timeToLive) {
        try {
            map.putAll(t, getCurrentTransaction(), timeToLive);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void clear() {
        try {
            map.clear();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void clear(boolean clearMaster) {
        try {
            map.clear(clearMaster);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Set keySet() {
        try {
            return map.keySet();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Collection values() {
        try {
            return map.values();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Set entrySet() {
        try {
            return map.entrySet();
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }


    // Cache methods
    public void addListener(CacheListener cacheListener) {
        map.addListener(cacheListener);
    }

    public boolean evict(Object key) {
        try {
            return map.evict(key);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Map getAll(Collection keys) throws CacheException {
        try {
            return map.getAll(keys);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public CacheEntry getCacheEntry(Object key) {
        try {
            return map.getCacheEntry(key);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void load(Object key) throws CacheException {
        try {
            map.load(key);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void loadAll(Collection keys) throws CacheException {
        try {
            map.load(keys);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public Object peek(Object key) {
        try {
            return map.peek(key);
        } catch (Exception e) {
            throw exTranslator.translate(e);
        }
    }

    public void removeListener(CacheListener cacheListener) {
        map.removeListener(cacheListener);
    }

    // Support methods

    public Transaction getCurrentTransaction() {
        Transaction.Created txCreated = txProvider.getCurrentTransaction(this, map.getMasterSpace());
        if (txCreated != null) {
            return txCreated.transaction;
        }
        return null;
    }

    public LockHandle lock(Object key) {
        return lock(key, defaultLockTimeToLive, defaultWaitingForLockTimeout);
    }

    public LockHandle lock(Object key, long lockTimeToLive, long waitingForLockTimeout) {
        return lockManager.lock(key, lockTimeToLive, waitingForLockTimeout);
    }

    public void unlock(Object key) {
        lockManager.unlock(key);
    }

    public boolean isLocked(Object key) {
        return lockManager.islocked(key);
    }

    public void putAndUnlock(Object key, Object value) {
        lockManager.putAndUnlock(key, value);
    }

    public long getDefaultTimeToLive() {
        return this.defaultTimeToLive;
    }

    public long getDefaultWaitForResponse() {
        return this.defaultWaitForResponse;
    }

    /**
     * Gets the isolation level from the current running transaction (enabling the usage of Spring
     * declarative isolation level settings). If there is no transaction in progress or the
     * transaction isolation is {@link org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT}
     * will use the default isolation level associated with this class (which is
     * <code>REPEATABLE_READ</code>).
     */
    public int getModifiersForIsolationLevel() {
        int isolationLevel = txProvider.getCurrentTransactionIsolationLevel(this);
        if (isolationLevel == TransactionDefinition.ISOLATION_DEFAULT) {
            return defaultIsolationLevel;
        } else if (isolationLevel == TransactionDefinition.ISOLATION_READ_UNCOMMITTED) {
            return ReadModifiers.DIRTY_READ;
        } else if (isolationLevel == TransactionDefinition.ISOLATION_READ_COMMITTED) {
            return ReadModifiers.READ_COMMITTED;
        } else if (isolationLevel == TransactionDefinition.ISOLATION_REPEATABLE_READ) {
            return ReadModifiers.REPEATABLE_READ;
        } else {
            throw new IllegalArgumentException("GigaSpaces does not support isolation level [" + isolationLevel + "]");
        }
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
