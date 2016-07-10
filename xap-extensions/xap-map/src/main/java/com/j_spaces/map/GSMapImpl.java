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

package com.j_spaces.map;

import com.gigaspaces.internal.client.cache.SpaceCacheException;
import com.gigaspaces.internal.client.spaceproxy.AbstractSpaceProxy;
import com.gigaspaces.internal.client.utils.SerializationUtil;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.CacheException;
import com.j_spaces.core.client.CacheTimeoutException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.javax.cache.CacheEntry;
import com.j_spaces.javax.cache.CacheListener;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class provides an implementation to GigaSpaces IMap interface, for Distributed Caching using
 * {@link java.util.Map} API. Simply call Map.get and Map.put methods to store and retrieve values
 * into or from the cache.
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/
final public class GSMapImpl extends AbstractMap {

    /**
     * GSMapImpl constructor. _serializationType
     *
     * @param space      Space proxy or DCache proxy.
     * @param timeToLive Time to live for <code>value</code> object in cache, use -1 for default
     *                   value.
     * @param txn        if not <code>null</code> all operation will be executed with the desired
     *                   transaction.
     * @param compress   the compress level can be 1-9
     *
     *                   To set new transaction use: setTransaction(txn) To reset the transaction
     *                   use: setTransaction(null) To get the current transaction use:
     *                   getTransaction()
     **/
    protected GSMapImpl(IJSpace space, long timeToLive, Transaction txn, int compress) {
        super(space, timeToLive, txn, compress, space.isOptimisticLockingEnabled());
        MapEntryFactory.initialize(space);
    }

    /**
     * GSMap constructor.
     *
     * @param space Space proxy or DCache proxy.
     **/
    public GSMapImpl(IJSpace space) {
        this(space, -1, null, NONE_COMPRESSION);
    }

    /**
     * GSMap constructor.
     *
     * @param space      Space proxy or DCache proxy.
     * @param timeToLive Time to live for <code>value</code> object in cache.
     **/
    public GSMapImpl(IJSpace space, long timeToLive) {
        this(space, timeToLive, null, NONE_COMPRESSION);
    }

    /**
     * GSMap constructor.
     *
     * @param space    Space proxy or DCache proxy.
     * @param compress Compress <code>value</code> object in cache.
     **/
    public GSMapImpl(IJSpace space, int compress) {
        this(space, Lease.FOREVER, null, compress);
    }

    /**
     * GSMap constructor.
     *
     * @param space Space proxy or DCache proxy.
     * @param txn   if not <code>null</code> all operation will be executed with the desired
     *              transaction. To set new transaction use: setTransaction(txn) To reset the
     *              transaction use: setTransaction(null) To get the current transaction use:
     *              getTransaction()
     **/
    public GSMapImpl(IJSpace space, Transaction txn) {
        this(space, Lease.FOREVER, txn, NONE_COMPRESSION);
    }

    /**
     * Puts <code>value</code> to the cache with specified <code>key</code> and
     * <code>attributes</code>  for <code>timeToLive</code> milliseconds to live in the cache.  If
     * there is no <code> value</code> with specified <code>key</code> in the cache then
     * <code>value</code> will be written to the cache first time otherwise <code>value</code> will
     * be updated.
     *
     * @param key        key for the <code>value</code>
     * @param value      object(~ entry)
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @param timeout    attributes to associate the <code>key</code> with
     * @return previous value associated with specified key.
     * @throws CacheException if error accord during put
     **/
    @Override
    protected Object internalPut(Object key, Object value, Transaction txn, long timeToLive, long timeout)
            throws CacheException {
        SpaceMapEntry newEnvelope = buildEnvelope(key, value, null);

        try {
            SpaceMapEntry oldEntry = (SpaceMapEntry) _spaceProxy.update(newEnvelope, txn, timeToLive,
                    timeout, UpdateModifiers.UPDATE_OR_WRITE);

            Object envValue = null;
            if (oldEntry != null)   // then update operation was performed, set old EntryInfo for old value
            {
                /** update the updated envelope value */
                // get envelope value and update the latest version
                envValue = oldEntry.getValue();
                if (_isVersioned)
                    _entryInfos.setEntryVersion(envValue, key, oldEntry.getVersion());
            }   // otherwise, a write operation was performed

            /** update the envelope value which was passed as parameter */
            if (_isVersioned)
                _entryInfos.setEntryVersion(value, key, newEnvelope.getVersion());

            return envValue;
        } catch (OperationTimeoutException e) {
            throw new CacheTimeoutException(key);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to put value in space cache", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Object internalRemove(Object key, Transaction txn, long waitForResponse) throws CacheException {
        return internalGet(key, txn, waitForResponse, ReadModifiers.REPEATABLE_READ, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        try {
            Object template = MapEntryFactory.create(key);
            /* TODO [@author Moran, BugId: GS1006]
             * since count(template, txn) ignores template with entry Uid,
			 * we will use read until this is resolved. */
            //return _spaceProxy.count( keyEnv, _transaction ) > 0;
            return _spaceProxy.read(template, _transaction, IJSpace.NO_WAIT, ReadModifiers.MATCH_BY_ID) != null;
        } catch (Exception e) {
            throw new SpaceCacheException("Failed on check if space cache contains key", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        try {
            // compress if needed
            Object compressedValue = SerializationUtil.serializeFieldValue(value, _compression);
            Object template = MapEntryFactory.create((Object) null, compressedValue);
            return _spaceProxy.count(template, _transaction) > 0;
        } catch (Exception e) {
            throw new SpaceCacheException("Failed on check if space cache contains value", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Object internalGet(Object key, Transaction txn, long waitForResponse, int readModifiers) {
        return internalGet(key, txn, waitForResponse, readModifiers, false);
    }

    /**
     * Gets or Removes the mapping for this key from this map if present (optional operation).
     * Client will wait at most <code>waitForResponse</code> milliseconds for get call to return.
     *
     * @param isRemove <code>true</code> if remove operation, otherwise false;
     * @return the value
     **/
    protected Object internalGet(Object key, Transaction txn, long waitForResponse,
                                 int readModifiers, boolean isRemove) {
        try {
            final SpaceMapEntry template = MapEntryFactory.create(key);
            final boolean ifExists = txn != null;
            SpaceMapEntry envelopeObj;
            if (isRemove)
                envelopeObj = (SpaceMapEntry) _spaceProxy.take(template, txn, waitForResponse, ReadModifiers.MATCH_BY_ID, ifExists);
            else
                envelopeObj = (SpaceMapEntry) _spaceProxy.read(template, txn, waitForResponse, readModifiers | ReadModifiers.MATCH_BY_ID, ifExists);

            if (envelopeObj == null) {
                if (txn == null)
                    return null;

                throw new CacheTimeoutException(key);
            }

            Object envValue = envelopeObj.getValue();
            envValue = SerializationUtil.deSerializeFieldValue(envValue, _compression);

            if (_isVersioned)
                _entryInfos.setEntryVersion(envValue, key, envelopeObj.getVersion());

            return envValue;
        } catch (EntryNotInSpaceException ex) {
            return null;
        } catch (CacheException e) {
            throw e;
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to get value from space cache", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        clear(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear(boolean clearMaster) {
        try {
            Object template = MapEntryFactory.create();
            if (_spaceProxy instanceof AbstractSpaceProxy && ((AbstractSpaceProxy) _spaceProxy).isCacheContainer()) {
                ((AbstractSpaceProxy) _spaceProxy).getLocalSpace().clear(template, null);

                if (clearMaster)
                    ((AbstractSpaceProxy) _spaceProxy).getRemoteSpace().clear(template, null);
            } else {
                _spaceProxy.clear(template, null);
            }
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to clear space cache", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Collection values() {
        SpaceMapEntry keyEnv = MapEntryFactory.create();
        SpaceMapEntry[] entries;
        try {
            entries = (SpaceMapEntry[]) _spaceProxy.readMultiple(keyEnv, _transaction, Integer.MAX_VALUE);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to read cache entries from remote space", e);
        }
        ArrayList<Object> values = new ArrayList<Object>(entries.length);

        for (SpaceMapEntry entry : entries)
            values.add(entry.getValue());

        return values;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set entrySet() {
        SpaceMapEntry keyEnv = MapEntryFactory.create();
        SpaceMapEntry[] entries;
        try {
            entries = (SpaceMapEntry[]) _spaceProxy.readMultiple(keyEnv, _transaction, Integer.MAX_VALUE);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to read cache entries from remote space", e);
        }
        HashSet<Map.Entry<Object, Object>> entriesSet = new HashSet<Map.Entry<Object, Object>>();

        for (SpaceMapEntry entry : entries) {
            Map.Entry<Object, Object> mapEntry = new SimpleEntry(entry.getKey(), entry.getValue(), entry.getVersion());
            entriesSet.add(mapEntry);
        }

        return entriesSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set keySet() {
        SpaceMapEntry keyEnv = MapEntryFactory.create();
        SpaceMapEntry[] entries;
        try {
            entries = (SpaceMapEntry[]) _spaceProxy.readMultiple(keyEnv, _transaction, Integer.MAX_VALUE);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to read cache entries from remote space", e);
        }

        HashSet<Object> keys = new HashSet<Object>();
        for (SpaceMapEntry entry : entries)
            keys.add(entry.getKey());

        return keys;
    }

    /**
     * <b>Unsupported Operation!</b> {@inheritDoc}
     */
    @Override
    final public void addListener(CacheListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public boolean evict(Object key) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public CacheEntry getCacheEntry(Object key) {
        Object value = get(key);
        long version = _entryInfos.getEntryVersion(value, key);
        return new SimpleEntry(key, value, version);
    }

    //   /**
    //    * <b>Unsupported Operation!</b>
    //    * {@inheritDoc}
    //    */
    //   final public CacheStatistics getCacheStatistics()
    //   {
    //      throw new UnsupportedOperationException();
    //   }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object peek(Object key) {
        return get(key);
    }

    /**
     * <b>Unsupported Operation!</b> {@inheritDoc}
     */
    @Override
    public void removeListener(CacheListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * @author Guy Korland
     * @version 1.0
     * @since 5.0
     */
    private static class SimpleEntry implements CacheEntry {

        private Object _key;
        private Object _value;
        private long _version;

        public SimpleEntry(Object key, Object value, long version) {
            _key = key;
            _value = value;
            _version = version;
        }

        public Object getKey() {
            return _key;
        }

        public Object getValue() {
            return _value;
        }

        public Object setValue(Object value) {
            Object oldValue = _value;
            _value = value;
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry e = (Map.Entry) o;
            return eq(_key, e.getKey()) && eq(_value, e.getValue());
        }

        @Override
        public int hashCode() {
            return ((_key == null) ? 0 : _key.hashCode()) ^
                    ((_value == null) ? 0 : _value.hashCode());
        }

        @Override
        public String toString() {
            return _key + "=" + _value;
        }

        private static boolean eq(Object o1, Object o2) {
            return (o1 == null ? o2 == null : o1.equals(o2));
        }

        @Override
        public long getCacheEntryVersion() {
            return _version;
        }
    }


}