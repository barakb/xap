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

import com.j_spaces.core.IJSpace;
import com.j_spaces.javax.cache.Cache;

import net.jini.core.transaction.Transaction;

import java.util.Map;


/**
 * <b>Notice: since 7.0 this API is internal and might be subject to changes in future versions. <p>
 * Use {@link org.openspaces.core.GigaMap} instead.</b> <p>
 *
 * This interface extends {@link java.util.Map} interface and provides Map interface to GigaSpaces.
 * The IMap interface can be used in remote , embedded and Master-Local Topology.<br> The Map
 * interface provides three collection views, which allow a map's contents to be viewed as a set of
 * keys, collection of values, or set of key-value mappings.<p> You can associate attributes to a
 * key and values (e.g. security tags, special user profiles, etc.), and set time for entries to
 * exist in the cache until eviction.
 *
 * @version 6.0
 * @see org.openspaces.core.GigaMap
 * @since 3.2
 */
public interface IMap extends Cache {
    /**
     * Sets the duration that entries will stay in the cache.
     *
     * @param millisecond how long entries will live in the cache,  in milliseconds
     */
    void setTimeToLive(long millisecond);

    /**
     * Returns how long cached objects will live in the cache.
     *
     * @return how long entries will live in the cache,  in milliseconds
     */
    long getTimeToLive();

    /**
     * Sets the timeout for the {@link #put}, {@link #get} and {@link #remove} calls to wait for
     * response.
     *
     * @param timeout time to wait for response
     */
    void setWaitForResponse(long timeout);

    /**
     * Returns how long wait for response for the {@link #put}, {@link #get} and {@link #remove}
     * calls.
     *
     * @return time to wait for response
     **/
    long getWaitForResponse();

    /**
     * Puts <code>value</code> to the cache with specified <code>key</code> for
     * <code>timeToLive</code> milliseconds to live in the cache.
     *
     * @param key        key for the <code>value</code>
     * @param value      object(~ entry)
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     */
    Object put(Object key, Object value, long timeToLive);

    /**
     * Puts <code>value</code> to the cache with specified <code>key</code> for
     * <code>timeToLive</code> milliseconds to live in the cache.
     *
     * @param key        key for the <code>value</code>
     * @param value      object(~ entry)
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     */
    Object put(Object key, Object value, long timeToLive, long timeout);

    /**
     * Puts <code>value</code> to the cache with specified <code>key</code> for
     * <code>timeToLive</code> milliseconds to live in the cache.
     *
     * @param key        key for the <code>value</code>
     * @param value      object(~ entry)
     * @param txn        the transaction object, if any, under which to perform the put
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     * @since 6.0
     */
    Object put(Object key, Object value, Transaction txn, long timeToLive);

    /**
     * Puts <code>value</code> to the cache with specified <code>key</code> for
     * <code>timeToLive</code> milliseconds to live in the cache.
     *
     * @param key             key for the <code>value</code>
     * @param value           object(~ entry)
     * @param txn             the transaction object, if any, under which to perform the put
     * @param timeToLive      time to keep object in this cache, in milliseconds
     * @param waitForResponse time to wait for response
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     * @since 7.1
     */
    Object put(Object key, Object value, Transaction txn, long timeToLive, long waitForResponse);

    /**
     * Copies all of the mappings from the specified map to the cache.
     *
     * @param map        A map of key and values to be copy into the cache
     * @param txn        the transaction object, if any, under which to perform the put
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @since 6.0
     */
    void putAll(Map map, Transaction txn, long timeToLive);

    /**
     * Returns the object that is associated with <code>key</code> in this cache. Client will wait
     * at most <code>waitForResponse</code> milliseconds for get call to return.
     *
     * @param key             key whose associated value is to be returned.
     * @param waitForResponse time to wait for response
     * @return Returns the object that is associated with the key
     */
    Object get(Object key, long waitForResponse);

    /**
     * Returns the object that is associated with <code>key</code> in this cache. Client will wait
     * at most <code>waitForResponse</code> milliseconds for get call to return.
     *
     * @param key             key whose associated value is to be returned.
     * @param txn             the transaction object, if any, under which to perform the get
     * @param waitForResponse time to wait for response
     * @param readModifiers   one or a union of {@link com.j_spaces.core.client.ReadModifiers}.
     * @return Returns the object that is associated with the key
     * @since 6.0
     */
    Object get(Object key, Transaction txn, long waitForResponse, int readModifiers);

    /**
     * Removes the mapping for this <code>key</code> from this cache. Client will wait at most
     * <code>waitForResponse</code> milliseconds for get call to return.
     *
     * @param key             key whose associated value is to be returned.
     * @param waitForResponse time to wait for response
     * @return The removed object
     **/
    Object remove(Object key, long waitForResponse);

    /**
     * Removes the mapping for this <code>key</code> from this cache. Client will wait at most
     * <code>waitForResponse</code> milliseconds for get call to return.
     *
     * @param key             key whose associated value is to be returned.
     * @param txn             the transaction object, if any, under which to perform the remove
     * @param waitForResponse time to wait for response
     * @return The removed object
     * @since 6.0
     **/
    Object remove(Object key, Transaction txn, long waitForResponse);


    // TODO and the associated CacheStatistics object.

    /**
     * The clear method will remove all objects from the cache including the key and the associated
     * value.
     *
     * @param clearMaster if <code>true</code> clear also master, when <code>false</code> equals to
     *                    clear()
     */
    void clear(boolean clearMaster);

    /**
     * Set current transaction, all the map calls will be performed under this transaction. Setting
     * to <code>null</code> will turn all the following map calls to be with transaction.
     *
     * In order to
     *
     * <pre><code>
     *
     * IMap map = ... ;
     * TransactionManager tm = ... ;
     *
     * Transaction.Created created = TransactionFactory#create(tm, 10000);
     * map.setTransaction(Created.transaction)
     *
     * map.put( "key1", "value1"); // under transaction
     * map.put( "key2", "value2"); // under transaction
     *
     * Created.transaction.commit();
     *
     * map.setTransaction( null);
     *
     * map.remove( "key2"); // not under transaction
     * </code></pre>
     *
     * @param transaction active transaction or <code>null</code>
     * @deprecated use explicit transaction instead
     **/
    @Deprecated
    void setTransaction(Transaction transaction);

    /**
     * Returns active transaction or <code>null</code> if no transaction defined.
     *
     * @return Transaction
     * @deprecated use explicit transaction instead
     **/
    @Deprecated
    Transaction getTransaction();

    /**
     * Returns the Master Space proxy.
     *
     * @return the master space proxy
     */
    IJSpace getMasterSpace();

    /**
     * Returns the Local Space proxy if exists.
     *
     * @return the local space proxy
     */
    IJSpace getLocalSpace();


    /**
     * Sets optimistic locking mode.
     *
     * @param versioned <code>true</code> if optimistic locking is needed
     */
    void setVersioned(boolean versioned);

    /**
     * Check if using optimistic locking.
     *
     * @return <code>true</code> if using optimistic locking
     */
    boolean isVersioned();
}
