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



/*
 * Created on 02/06/2005
 */
package com.j_spaces.javax.cache;


import java.util.Collection;
import java.util.Map;
import java.util.Set;

/*
 * TODO add this on the next version at the end 
 * Mainly it represents the object statistics. "CacheStatistics" represents the read-only 
 * statistics of the cache, while "CacheAttributes" represents the user settable attributes 
 * of the cache. 
 */

/**
 * A cache, being a mechanism for efficient temporary storage of objects for the purpose of
 * improving the overall performance of an application system.<p> A cache could be scoped, for
 * examples to a JVM, all JVMs on a node, all nodes in a cluster, etc. Operations that are scoped to
 * a cache such as put or load would affect all JVMs in the cache. So the object loaded in 1 JVM
 * would be equally available to all other JVMs in the cache.<p> Objects are identified in the cache
 * by a key. A key can be any Java object that implements the uniquely the <code>toString()</code>
 * method. If the object is to be distributed or persisted (if supported) it must implement
 * serializable. Each object in the cache will have a <code>CacheEntry</code> object associated with
 * it. This object will encapsulate the metadata associated with the cached object. <p> <b>All the
 * methods that are inherited from Map might throw {@link com.j_spaces.core.client.CacheException
 * com.j_spaces.core.client.CacheException}. This Exception extends {@link
 * java.lang.RuntimeException RuntimeException} in order to keep the {@link java.util.Map Map} API.
 * </b>
 *
 * @see com.j_spaces.javax.cache.CacheException
 * @see com.j_spaces.javax.cache.CacheEntry
 * @deprecated
 */
@Deprecated
public interface Cache extends java.util.Map {
    /**
     * Add a listener to the list of cache listeners.<br> <b>Optional, not supported by all the
     * implementations, currently supported only by local cache.</b>
     *
     * @param listener call back <code>CacheListener</code>
     * @throws UnsupportedOperationException can be thrown by implementations that doesn't support
     *                                       this method
     */
    void addListener(CacheListener listener);
   
   
   /*
    * TODO add this on the next version at the end 
    *  and the associated CacheStatistics object.
    */

    /**
     * The clear method will remove all objects from the cache including the key and the associated
     * value.
     */
    void clear();

    /**
     * Returns true if the cache contains the specified key. The search is scoped to the cache.
     * Other caches in the system will not be searched and a CacheLoader will not be called.
     *
     * @param key key whose presence in this cache is to be tested.
     * @return <code>true</code>, if the cache contains the specified key.
     */
    boolean containsKey(Object key);

    /**
     * Returns true if this cache maps one or more keys to the specified value. The search is scoped
     * to the cache. Other caches in the system will not be searched and a CacheLoader will not be
     * called.
     *
     * @param value value whose presence in this cache is to be tested.
     * @return <code>true</code>, if the cache contains the specified value.
     */
    boolean containsValue(Object value);

    /**
     * Returns a set view of the objects currently contained in the cache. The search is scoped to
     * the cache. Other caches in the system will not be searched and a CacheLoader will not be
     * called.
     *
     * @return a set of CacheEntry
     */
    Set entrySet();

    /**
     * Equality is based on the Set returned by entrySet.
     *
     * @param obj the Cache to check
     * @return <code>true</code>, if two caches returns the same <code>entrySet()</code>.
     */
    boolean equals(Object obj);

    /**
     * The evict method will remove object from the cache the matches the provided key.
     *
     * @param key the key to evict
     * @return <code>true</code>, if the key was evicted
     */
    boolean evict(Object key);

   /*
    * TODO add in future versions
    * If the "arg" argument is set, 
    * the arg object will be passed to the CacheLoader.load method. 
    * The cache will not dereference the object. 
    * If no "arg" value is provided a null will be passed to the load method. 
    * The storing of null values in the cache is permitted, however, 
    * the get method will not distinguish returning a null stored in the cache and not 
    * finding the object in the cache. In both cases a null is returned.
    */

    /**
     * The get method will return, from the cache, the object associated with the argument "key". If
     * the object is not in the cache, the associated cache loader will be called. If no loader is
     * associated with the object, a null is returned. If a problem is encountered during the
     * retrieving or loading of the object, an exception (to be defined) will be thrown.
     *
     * @param key key whose associated value is to be returned.
     * @return the value to which this cache maps the specified key, or <tt>null</tt> if the cache
     * contains no mapping for this key.
     */
    Object get(Object key);
   
   /*
    *  TODO add in future versions
    * If the "arg" argument is set, the arg object will 
    * be passed to the CacheLoader.loadAll method. The cache will not dereference the 
    * object. If no "arg" value is provided a null will be passed to the loadAll method. 
    * The storing of null values in the cache is permitted, however, the get method will 
    * not distinguish returning a null stored in the cache and not finding the object in 
    * the cache. In both cases a null is returned.
    */

    /**
     * The getAll method will return, from the cache, a Map of the objects associated with the
     * Collection of keys in argument "keys". If the objects are not in the cache, the associated
     * cache loader will be called. If no loader is associated with an object, a null is returned.
     * If a problem is encountered during the retrieving or loading of the objects, an exception
     * will be thrown.
     *
     * @param keys keys whose associated values are to be returned.
     * @return Map of the objects associated with the Collection of keys
     * @throws CacheException error accord while loading
     */
    Map getAll(Collection keys) throws CacheException;

    /**
     * Returns the CacheEntry object associated with the object identified by "key".
     *
     * @param key key whose associated value is to be returned.
     * @return the CacheEntry to which this cache maps the specified key, or <tt>null</tt> if the
     * cache contains no mapping for this key.
     */
    CacheEntry getCacheEntry(Object key);

//   /** TODO add 
//    * returns the CacheStatistics object associated with the cache.*/
//   CacheStatistics 	getCacheStatistics();

    /**
     * Returns the hash code value for this cache.
     *
     * @return the hash code
     */
    int hashCode();

    /**
     * Return true if entrySet().isEmpty() return true.
     *
     * @return <code>true</code> if empty
     */
    boolean isEmpty();

    /**
     * Returns a set view of the keys currently contained in the cache. The search is scoped to the
     * cache. Other caches in the system will not be searched and a CacheLoader will not be called.
     *
     * @return set of the keys
     */
    Set keySet();
   
   /* TODO in future versions
    * If a problem is encountered 
    *  during the retrieving or loading of the object, an exception should be logged. 
    *  If the "arg" argument is set, the arg object will be passed to the CacheLoader.
    *  load method. The cache will not dereference the object. If no "arg" value 
    *  is provided a null will be passed to the load method. The storing of null values
    *  in the cache is permitted, however, the get method will not distinguish returning
    *  a null stored in the cache and not finding the object in the cache. In both
    *  cases a null is returned. 
    */

    /**
     * The load method provides a means to "pre load" the cache. This method will, load the
     * specified object into the cache using the associated CacheLoader. If the object already
     * exists in the cache, no action is taken. If no loader is associated with the object, no
     * object will be loaded into the cache.
     *
     * @param key key whose associated value is to be loaded.
     * @throws CacheException error accord while loading
     **/
    void load(Object key) throws CacheException;
   
   /* TODO
    * If a problem is encountered during the retrieving or loading of the objects, 
    * an exception (to be defined) should be logged. The getAll method will return, 
    * from the cache, a Map of the objects associated with the Collection of keys 
    * in argument "keys". If the objects are not in the cache, the associated cache 
    * loader will be called. If no loader is associated with an object, a null is returned. 
    * If a problem is encountered during the retrieving or loading of the objects, 
    * an exception (to be defined) will be thrown. If the "arg" argument is set, 
    * the arg object will be passed to the CacheLoader.loadAll method. 
    * The cache will not dereference the object. If no "arg" value is provided 
    * a null will be passed to the loadAll method.
    */

    /**
     * The loadAll method provides a means to "pre load" objects into the cache. This method will,
     * load the specified objects into the cache using the associated cache loader. If the an object
     * already exists in the cache, no action is taken. If no loader is associated with the object,
     * no object will be loaded into the cache.
     *
     * @param keys keys' Collection whose associated value to be loaded using the associated
     *             CacheLoader if this cache doesn't contain it.
     * @throws CacheException error accord while loading
     */
    void loadAll(Collection keys) throws CacheException;

    /**
     * The peek method will return the object associated with "key" if it currently exists (and is
     * valid) in the cache. The search is scoped to the cache. Other caches in the system will not
     * be searched and a CacheLoader will not be called.
     *
     * @param key key whose associated value is to be peek.
     * @return the value to which this cache maps the specified key, or <tt>null</tt> if the cache
     * contains no mapping for this key.
     */
    Object peek(Object key);

    /**
     * The put method adds the object "value" to the cache identified by the object "key".
     *
     * @param key   key with which the specified value is to be associated.
     * @param value value to be associated with the specified key.
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     */
    Object put(Object key, Object value);

    /**
     * Copies all of the mappings from the specified map to the cache.
     *
     * @param map A map of key and values to be copy into the cache
     */
    void putAll(Map map);

    /**
     * The remove method will delete the object from the cache including the key, the associated
     * value and the associated CacheStatistics object.
     *
     * @param key key whose associated value is to be removed.
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     */
    Object remove(Object key);

    /**
     * Remove a listener from the list of cache listeners.<br> <b>Optional, not supported by all the
     * implementations, currently supported only by local cache</b>
     *
     * @param listener the <code>CacheListener</code> to remove
     * @throws UnsupportedOperationException can be thrown by implementations that doesn't support
     *                                       this method
     */
    void removeListener(CacheListener listener);

    /**
     * Returns the size of this map.
     *
     * The search is scoped to the cache. Other caches in the system will not be searched and a
     * CacheLoader will not be called.
     *
     * @return the number of key-value mappings in this cache.
     */
    int size();

    /**
     * Returns a collection view of the values contained in this cache.
     *
     * The search is scoped to the cache. Other caches in the system will not be searched and a
     * CacheLoader will not be called.
     *
     * @return a collection view of the values contained in this cache.
     */
    Collection values();
}
