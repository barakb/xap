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
 * Title:		HashedRwlStoredList.java
 * Description:	Hash-backed Read/Write Lock version of StoredList
 * Company:		GigaSpaces Technologies
 * 
 * @author      Yechiel Fefer
 * @version		1.0 21/06/2005
 * @since		5.2 
 */

package com.j_spaces.kernel;

import java.util.HashMap;

/**
 * hash-backed Read/Write Lock version of StoredList to be used with JDK 1.5+
 */
@com.gigaspaces.api.InternalApi
public class HashedSimpleLockIStoredList<T>
        extends SimpleLockStoredList<T> {
    private final static int MINIMAL_SIZE_TO_CREATE_HASH = 10;

    //hash set contains mapping from the object to ObjectInfo
    //note- hashCode & equals for the stored object must be based on physical identity,
    //so the hashset is manipulated based on the ObjectInfo->subject physical identity and
    private HashMap<T, IObjectInfo<T>> _objectsMap;

    public HashedSimpleLockIStoredList(boolean Support_Random_Scans) {
        super(Support_Random_Scans);

    }


    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {
        lock.lock();
        try {

            IObjectInfo<T> oi = store_impl(subject);
            updateHashAfterInsertion(oi);
            return oi;
        } finally {
            lock.unlock();
        }
    }


    //inserted an object , use the oi to update the hash if need to
    private void updateHashAfterInsertion(IObjectInfo<T> oi) {
        if (_objectsMap != null) {
            _objectsMap.put(oi.getSubject(), oi);
        } else {
            if (m_Size >= MINIMAL_SIZE_TO_CREATE_HASH) {
                _objectsMap = new HashMap<T, IObjectInfo<T>>();
                ObjectInfo<T> toi = m_Tail;

                while (toi != null) {
                    if (toi.getSubject() != null)
                        _objectsMap.put(toi.getSubject(), toi);
                    toi = toi.getForwardRef();
                }
            }
        }

    }


    /**
     * remove an element described by ObjectInfo
     */
    public void remove(IObjectInfo<T> oi) {
        lock.lock();
        try {
            if (_objectsMap != null && oi.getSubject() != null) {
                _objectsMap.remove(oi.getSubject());
            }
            remove_impl((ObjectInfo<T>) oi);
            if (m_Size == 0 && _objectsMap != null)
                _objectsMap = null;
        } finally {
            lock.unlock();
        }

    }


    /**
     * given an object scan the list, find it and remove it, returns true if found
     */
    public boolean removeByObject(T obj) {
        lock.lock();
        IObjectInfo<T> oi = null;
        try {
            if (_objectsMap != null) {
                if ((oi = _objectsMap.remove(obj)) != null) {
                    remove_impl((ObjectInfo<T>) oi);
                    if (m_Size == 0)
                        _objectsMap = null;
                    return true;
                }
                return false;
            }
            return removeByObject_impl(obj);
        } finally {
            lock.unlock();
        }
    }

    /**
     * is this object contained in the SL ?
     */
    public boolean contains(T obj) {
        lock.lock();
        try {
            if (_objectsMap != null) {
                return _objectsMap.containsKey(obj);
            }
            return contains_impl(obj);
        } finally {
            lock.unlock();
        }
    }


}
