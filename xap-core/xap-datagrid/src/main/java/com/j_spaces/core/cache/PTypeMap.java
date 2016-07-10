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
 * Title:		 PTypeMap.java
 * Description: PType mapping from <K:int,V:PType>
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 04/07/2005
 * @since		 4.1 Build#1202
 */
package com.j_spaces.core.cache;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PType mapping from <K:int, V:PType> as a resizable-array implementation. Implements an API
 * similar to {@linkplain java.util.Map Map}.
 */
@com.gigaspaces.api.InternalApi
public class PTypeMap {
    /**
     * The array buffer into which the elements of the ArrayList are stored. The capacity of the
     * ArrayList is the length of this array buffer.
     */
    private volatile TypeData[] elementData;

    // Locks between increase of data length and put/removal operations
    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock touchLock = rwl.readLock();    //in-range lock
    private final Lock modifyLock = rwl.writeLock();  //not in-range lock

    public PTypeMap() {
        this.elementData = new TypeData[10];
    }

    public TypeData get(IServerTypeDesc typeDesc) {
        try {
            return elementData[typeDesc.getTypeId()];
        } catch (IndexOutOfBoundsException ioobe) {
            return null;
        }
    }

    public void put(IServerTypeDesc typeDesc, TypeData element) {
        final int key = typeDesc.getTypeId();

        try {
            touchLock.lock();
            ensureCapacity(key);
            elementData[key] = element;
        } finally {
            touchLock.unlock();
        }
    }

    public void remove(IServerTypeDesc typeDesc) {
        final int key = typeDesc.getTypeId();
        if (key >= elementData.length)
            return;

        try {
            touchLock.lock();
            elementData[key] = null;
        } finally {
            touchLock.unlock();
        }
    }

    private void ensureCapacity(int capacity) {
        if (capacity >= elementData.length) {
            try {
                touchLock.unlock(); // must unlock first to obtain modification lock
                modifyLock.lock();

                increaseCapacity(capacity + 1);
            } finally {
                // downgrade lock
                touchLock.lock();      // acquire touch lock without giving up modification lock
                modifyLock.unlock();   // unlock modification lock, still hold touch lock
            }
        }
    }

    private void increaseCapacity(int minCapacity) {
        int oldCapacity = elementData.length;
        if (minCapacity > oldCapacity) {
            Object oldData[] = elementData;
            int newCapacity = (oldCapacity * 3) / 2 + 1;
            if (newCapacity < minCapacity)
                newCapacity = minCapacity;
            TypeData newData[] = new TypeData[newCapacity];
            System.arraycopy(oldData, 0, newData, 0, oldCapacity);
            elementData = newData; //volatile
        }
    }
}
