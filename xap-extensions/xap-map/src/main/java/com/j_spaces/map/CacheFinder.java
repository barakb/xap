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

import com.gigaspaces.internal.remoting.routing.clustered.LookupType;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;
import com.sun.jini.start.LifeCycle;

import java.util.Properties;

/**
 * <pre>
 *  This utility class provides accessing to cache {@link com.j_spaces.map.IMap IMap} API.
 *
 *  The CacheFinder is designed to provide a unified interface for finding a
 *  cache in each of the mode specified.
 *  To provide a unified manager of accessing the cache use a url based interface
 *  which will provide the information of the protocol and the address of the
 *  cache that should be found.
 *
 *  The general format for this URL is as follows:
 *
 *  <code>Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]</code>
 *
 *  Protocol: [ rmi | jini | java ]
 *
 *            The "java" protocol enables working with an embedded space behind the cache. When
 * choosing so
 *            one should specify in the query string one of three operations:
 *
 *            &gt; create - to create a new space
 *             example of &quot;mySpace&quot; space creation:
 *
 *            <code>java://myHost/myContinaer/mySpace?create</code>
 *
 *            &gt; open - To open  an existing space in an embedded mode:
 *             example of opening &quot;mySpace&quot; space:
 *
 *            <code>java://myHost/myContinaer/mySpace?open</code>
 *
 *            &gt; destroy - for destroy  an existing space:
 *             example of destroying &quot;mySpace&quot; space:
 *
 *             <code>java://myHost/myContinaer/mySpace?destroy</code>
 *
 *  Host: The host name. Can be '*', when JINI is used as a protocol, the host
 *        value determines whether to use Unicast or Multicast.
 *
 *  Port: the registry or lookup port. If the port is  not specified,
 *        the default port 10098 will be used.
 *
 *  Container Name: The name of the container, which holds the space.
 *                  If the container name is '*' then the container attribute will be ignored and
 * the
 *                  space will be looked for directly regardless of the container that holds it .
 *
 *  Query String: The query_string represents the cache type i.e.:
 *
 *       &gt; create - local cache
 *       <code>jini://myHost/myContinaer/mySpace?uselocalcache</code>
 *
 *       &gt; create - IMap based space interface
 *       <code>jini://myHost/myContinaer/mySpace</code>
 *
 *  Examples of space url's:
 *
 *  1. looking for a cache in rmi registry in a specific host and container.
 *      <code>rmi://my_container_host/my_containername/myspace?uselocalcache</code>
 *
 *  2. Looking for a cache using JINI Unicast protocol.
 *     <code>jini://mylookuphost/mycontainername/myspace?uselocalcache</code>
 *   Or
 *     <code>jini://mylookuphost/<tt>*</tt>/myspace?uselocalcache</code>
 *
 *  3. Looking for a cache using the JINI multicast protocol.
 *     <code>jini://<tt>*</tt>/<tt>*</tt>/containername/myspace?uselocalcache</code>
 *   Or
 *     <code>jini://<tt>*</tt>/<tt>*</tt>/myspace?uselocalcache</code>
 * </pre>
 *
 * @author Guy Korland
 * @version 1.0
 * @see SpaceURL
 * @since 5.0
 * @deprecated Use {@link org.openspaces.core.map.MapConfigurer} instead.
 */
@Deprecated
public class CacheFinder extends SpaceFinder {
    private static CacheFinder cacheFinder = new CacheFinder();

    /**
     * Singleton.
     */
    private CacheFinder() {
        super();
    }

    /**
     * <pre>
     *  The general format for this URL is as follows:
     *  Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]
     *  Protocol: [ RMI | JINI | JAVA | WS ]
     *  This method also supports multiple urls separate by &quot;;&quot;,
     *  e.i. rmi://localhost/containerName/SpaceName?uselocalcache;
     *  jini://localhost/containerName/SpaceName?uselocalcache;
     *  It is useful when Jini URL for locating services on the network is not available.
     *    If the first URL is unavailable, CacheFinder will try the next one until a live cache
     * will
     * be found.
     *  If all URLs are unavailable this method throws FinderException
     * </pre>
     *
     * @param urls the space url
     * @return Returns single cache.
     * @throws FinderException During finding cache.
     */
    public static Object find(String urls) throws FinderException {
        return cacheFinder.findService(urls, null, null, null);
    }

    @Override
    protected final Object findService(SpaceURL url, Properties customProperties, LifeCycle lifeCycle,
                                       CredentialsProvider credentialsProvider, long timeout, LookupType lookupType)
            throws FinderException {
        boolean isDcache = url.getProperty(SpaceURL.USE_LOCAL_CACHE, Boolean.FALSE.toString()).equalsIgnoreCase(Boolean.TRUE.toString());
        /* HACK:
        When finding a map without 'useLocalCache', we create a GSMapImpl over the resulting proxy.
        When finding a map with 'useLocalCache', we remove the 'useLocalCache' to obtain a regular proxy, and create
        a MapCache instead, which is a special Map impl with behaviour similar to local cache...
         */
        if (isDcache)
            url.remove(SpaceURL.USE_LOCAL_CACHE);
        IJSpace space = (IJSpace) super.findService(url, customProperties, lifeCycle, credentialsProvider, timeout, lookupType);

        if (isDcache) {
            String localCacheUpdateMode = url.getProperty(SpaceURL.LOCAL_CACHE_UPDATE_MODE);
            Integer updateMode = localCacheUpdateMode != null ? Integer.parseInt(localCacheUpdateMode) : null;
            return MapFactory.createMapWithCache(space, updateMode);
        } else {
            return MapFactory.createMap(space);
        }
    }
}
