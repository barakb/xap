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

package com.gigaspaces.lrmi;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Object Registry contains information about exported remote objects.
 *
 * @author Igor Goldenberg
 * @since 4.0
 **/
@com.gigaspaces.api.InternalApi
public class ObjectRegistry {
    public static class Entry {
        final public Remote m_Object;
        final public long m_ObjectId;
        private String _protocol;
        private ServerPeer _serverPeer;
        final private ClassLoader _exportedThreadClassLoader;
        final private LRMIMethod[] _methodList;

        public Entry(Remote object, long objectId, LRMIMethod[] lrmiMethods) {
            m_Object = object;
            m_ObjectId = objectId;
            _methodList = lrmiMethods;
            _exportedThreadClassLoader = Thread.currentThread().getContextClassLoader();
        }

        /**
         * Set protocol name.
         */
        public void setProtocol(String protocol) {
            _protocol = protocol;
        }

        /**
         * Set server-peer registration of exported object.
         */
        public void setServerPeer(ServerPeer serverPeer) {
            _serverPeer = serverPeer;
        }

        /**
         * @return server-peer registration of exported object
         */
        public ServerPeer getServerPeer() {
            return _serverPeer;
        }

        /**
         * @return the protocol name
         */
        public String getProtocol() {
            return _protocol;
        }

        /**
         * @return the thread context ClassLoader of the thread which call LRMIRuntime.export()
         */
        final public ClassLoader getExportedThreadClassLoader() {
            return _exportedThreadClassLoader;
        }

        /**
         * @return the exported object
         */
        final public Remote getObject() {
            return m_Object;
        }

        /**
         * @return the method list associated with this service
         */
        public LRMIMethod[] getMethodList() {
            return _methodList;
        }

        @Override
        public String toString() {
            return "Entry [ExportedThreadClassLoader="
                    + _exportedThreadClassLoader + ", Object=" + m_Object
                    + ", ObjectId=" + m_ObjectId + ", Protocol=" + _protocol
                    + ", ServerPeer=" + _serverPeer + ", methodList="
                    + _methodList + "]";
        }
    }

    final private ConcurrentHashMap<Remote, Entry> _objMap;    // maps remote objects to registry entries.
    final private ConcurrentHashMap<Long, Entry> _objIdMap;  // maps IDs of remote objects to registry entries.

    public ObjectRegistry() {
        _objMap = new ConcurrentHashMap<Remote, Entry>();
        _objIdMap = new ConcurrentHashMap<Long, Entry>();
    }

    /**
     * If protocol is not held by any other Object in this registry, the <code>true</code> is
     * returned; otherwise, there is at least one protocol registration that is being used.
     *
     * @param protocol LRMI protocol of the transport layer
     * @return <code>true</code> if last registrar of this protocol.
     */
    public boolean isLastRegistrar(String protocol) {
        if (_objMap.isEmpty())
            return true;

        List<Entry> entryValue = new ArrayList<Entry>(_objMap.values());
        for (Iterator<Entry> iter = entryValue.iterator(); iter.hasNext(); ) {
            Entry e = iter.next();
            if (e._protocol.equals(protocol))
                return false; // not the latest with this protocol
        }

        return true;
    }

    /**
     * Creates a new entry in this registry.
     */
    public Entry createEntry(Remote object, long objectId, LRMIMethod[] lrmiMethods) {
        Entry entry = new Entry(object, objectId, lrmiMethods);
        _objMap.put(object, entry);
        _objIdMap.put(objectId, entry);

        return entry;
    }

    /**
     * Returns the entry corresponding to the specified object.
     */
    public Entry getEntryFromObject(Object object) {
        return _objMap.get(object);
    }

    /**
     * Returns the entry corresponding to the specified object id.
     **/
    public Entry getEntryFromObjectIdIfExists(long objectId) {
        return _objIdMap.get(objectId);
    }

    public Entry getEntryFromObjectId(long objectId) throws NoSuchObjectException {
        Entry entry = _objIdMap.get(objectId);
        if (entry == null)
            throw new NoSuchObjectException("Object " + objectId + " not found in object registry");

        return entry;
    }

    /**
     * Removes the entry corresponding to the specified object.
     */
    public void removeEntry(Remote object) {
        Entry entry = _objMap.remove(object);
        _objIdMap.remove(entry.m_ObjectId);
    }

    /**
     * Clears the entire object registery
     */

    public void clear() {
        _objMap.clear();
        _objIdMap.clear();
    }
}