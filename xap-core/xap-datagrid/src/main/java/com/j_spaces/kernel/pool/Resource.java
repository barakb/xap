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
 * Title:        Resource.java
 * Description:  Resource class
 * Company:      GigaSpaces Technologies
 * 
 * @author		 Guy korland
 * @author       Moran Avigdor
 * @version      1.0 01/06/2005
 * @since		 4.1 Build#1173
 */

package com.j_spaces.kernel.pool;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Resources should extend this class and implement all abstract methods. When done using the
 * resource, it should be {@linkplain #release() released}.
 *
 * <br><br> <b>Usage:</b>
 * <code><pre>
 *    MyResourceFactory factory = new MyResourceFactory();
 *    ResourcePool pool = new ResourcePool(factory, 10, 50);
 *
 *    MyResource resource = pool.getResource();
 *    try {
 *    ...
 *    }finally {
 *      resource.release();
 *    }
 * </pre></code>
 */
public abstract class Resource implements IResource {
    private final AtomicBoolean _acquired = new AtomicBoolean(false);
    boolean _fromPool;

    /*
     * @see com.j_spaces.kernel.pool.IResource#setFromPool(boolean)
     */
    public void setFromPool(boolean fromPool) {
        _fromPool = fromPool;
    }

    /*
     * @see com.j_spaces.kernel.pool.IResource#isFromPool()
     */
    public boolean isFromPool() {
        return _fromPool;
    }

    /*
     * @see com.j_spaces.kernel.pool.IResource#acquire()
     */
    public boolean acquire() {
        return _acquired.compareAndSet(false, true);
    }

    /*
     * @see com.j_spaces.kernel.pool.IResource#setOccupied(boolean)
     */
    public void setAcquired(boolean acquired) {
        _acquired.set(acquired);
    }

    /*
     * @see com.j_spaces.kernel.pool.IResource#isOccupied()
     */
    public boolean isAcquired() {
        return _acquired.get();
    }

    /*
     * @see com.j_spaces.kernel.pool.IResource#release()
     */
    public void release() {
        clear();
        // sanity check: expected acquired = true
        if (!_acquired.compareAndSet(true, false)) {
            throw new RuntimeException("Resource " + getClass().getName() + " had already been released prior to call.");
        }
    }


    /*
     * @see com.j_spaces.kernel.pool.IResource#clear()
     */
    public abstract void clear();
}
