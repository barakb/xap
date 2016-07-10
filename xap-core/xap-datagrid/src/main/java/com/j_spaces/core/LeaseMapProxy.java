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

package com.j_spaces.core;

import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.time.SystemTime;

import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.LeaseMapException;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class LeaseMapProxy implements LeaseMap, Serializable {
    private static final long serialVersionUID = 3360605791400151942L;

    final private Map<Object, Long> m_Map;
    final private IRemoteSpace m_Origin;

    LeaseMapProxy(LeaseProxy lease, long duration) {
        m_Map = new HashMap<Object, Long>();
        m_Map.put(lease, duration);
        m_Origin = lease.getSpace();
    }

    public boolean canContainKey(Object obj) {
        if (!(obj instanceof LeaseProxy))
            return false;

        LeaseProxy l = (LeaseProxy) obj;
        return m_Origin.equals(l.getSpace());
    }


    public void cancelAll()
            throws LeaseMapException, RemoteException {
        if (m_Map.isEmpty())
            return;

        int c = 0;
        int mapSize = m_Map.keySet().size();
        LeaseProxy[] leaseArr = new LeaseProxy[mapSize];
        String[] entryUIDs = new String[mapSize];
        String[] classes = new String[mapSize];
        int[] objectTypes = new int[mapSize];

        // Build <code>leases</code>, <code>UID's</code> and <code>duration</code> array
        for (Iterator mapIter = m_Map.keySet().iterator(); mapIter.hasNext(); ) {
            leaseArr[c] = (LeaseProxy) mapIter.next();
            entryUIDs[c] = leaseArr[c].getUID();
            classes[c] = leaseArr[c].getClassname();
            objectTypes[c] = leaseArr[c].getObjectType();
            c++;
        }

        // Cancel all entries of LeaseMap
        Exception[] exceptions = m_Origin.cancelAll(entryUIDs, classes, objectTypes);

        Map<LeaseProxy, Exception> exceptionMap = new HashMap<LeaseProxy, Exception>();
        boolean exceptionThrown = false;

        for (int i = 0; i < exceptions.length; i++) {
            if (exceptions[i] != null) {
                m_Map.remove(leaseArr[i]);
                exceptionMap.put(leaseArr[i], exceptions[i]);
                exceptionThrown = true;
            }
        }

        if (exceptionThrown)
            throw new LeaseMapException("Some leases failed to cancel", exceptionMap);
    }


    public void renewAll()
            throws LeaseMapException, RemoteException {
        if (m_Map.isEmpty())
            return;

        int c = 0;
        int size = m_Map.size();
        LeaseProxy[] leaseArr = new LeaseProxy[size];
        String[] entryUIDs = new String[size];
        String[] classes = new String[size];
        int[] objectTypes = new int[size];
        long[] durations = new long[size];

        // Build <code>leases</code>, <code>UID's</code> and <code>duration</code> array
        for (Iterator mapIter = m_Map.keySet().iterator(); mapIter.hasNext(); ) {
            leaseArr[c] = (LeaseProxy) mapIter.next();
            durations[c] = m_Map.get(leaseArr[c]);
            entryUIDs[c] = leaseArr[c].getUID();
            classes[c] = leaseArr[c].getClassname();
            objectTypes[c] = leaseArr[c].getObjectType();
            c++;
        }

        Object[] result = m_Origin.renewAll(entryUIDs, classes, objectTypes, durations);
        Exception[] exceptions = (Exception[]) result[0];
        long[] newDurations = (long[]) result[1];

        HashMap<LeaseProxy, Exception> exceptionMap = new HashMap<LeaseProxy, Exception>();
        boolean exceptionThrown = false;
        long currentTime = SystemTime.timeMillis();

        for (int i = 0; i < exceptions.length; i++) {
            if (exceptions[i] != null) {
                m_Map.remove(leaseArr[i]);
                exceptionMap.put(leaseArr[i], exceptions[i]);
                exceptionThrown = true;
            } else {
                leaseArr[i].setExpiration(newDurations[i] + currentTime);
            }
        }

        if (exceptionThrown)
            throw new LeaseMapException("Some leases failed to renew", exceptionMap);
    }

    public void clear() {
        m_Map.clear();
    }

    public boolean containsKey(Object o) {
        return m_Map.containsKey(o);
    }

    public boolean containsValue(Object o) {
        return m_Map.containsValue(o);
    }

    public Set entrySet() {
        return m_Map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LeaseMap) {
            LeaseMap lm = (LeaseMap) o;
            return entrySet().equals(lm.entrySet());
        }

        return false;
    }

    public Object get(Object key) {
        return m_Map.get(key);
    }

    @Override
    public int hashCode() {
        return m_Map.hashCode();
    }

    public boolean isEmpty() {
        return m_Map.isEmpty();
    }

    public Set keySet() {
        return m_Map.keySet();
    }

    public Object put(Object key, Object value) {
        if (!canContainKey(key))
            throw new IllegalArgumentException(key.toString());

        if (!(value instanceof Long))
            throw new IllegalArgumentException(value.toString());

        return m_Map.put(key, (Long) value);
    }

    public void putAll(Map t) {
        Set<Map.Entry> set = t.entrySet();
        for (Map.Entry entry : set) {
            Object value = entry.getValue();
            if (!canContainKey(entry.getKey()) || !(value instanceof Long))
                throw new IllegalArgumentException(t.toString());
        }
        m_Map.putAll(t);
    }

    public Object remove(Object key) {
        return m_Map.remove(key);
    }

    public int size() {
        return m_Map.size();
    }

    public Collection values() {
        return m_Map.values();
    }
}