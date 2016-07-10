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

package com.j_spaces.kernel.pool;

/**
 * A resource interface required by a {@link ResourcePool}. {@link Resource} is the default
 * implementation. {@link IResource} may be used when dealing with hierarchy constraints. <p>
 * <b>Usage</b>: extend your resource class from {@link Resource} <code>public class myResource
 * extends Resource {...}</code> or if you have hierarchy constraints, implement {@link IResource}.
 *
 * @author moran
 * @version 1.0
 * @since 6.0.2
 */
public interface IResource {
    /**
     * An indication if this resource originated from the pool or was allocated on the fly. The
     * latter, will not return to the pool.
     *
     * @return <code>true</code> if originated from the pool; <code>false</code> if allocated.
     */
    abstract boolean isFromPool();

    /**
     * An indication if this resource originated from the pool or was allocated on the fly. The
     * latter, will not return to the pool.
     *
     * @param fromPool <code>true</code> if originated from the pool; <code>false</code> if not part
     *                 of the pool.
     */
    abstract void setFromPool(boolean fromPool);

    /**
     * An indication if this resource is currently acquired. Usually a resource becomes acquired
     * after a call to {@link #acquire()}, or by use of it's status indicator {@link
     * #setAcquired(boolean)}.
     *
     * @return <code>true</code> if resource is acquired. <code>false</code> if not yet acquired.
     */
    abstract boolean isAcquired();

    /**
     * An indication if this resource is acquired.
     *
     * @param acquired <code>true</code> if resource is acquired. <code>false</code> if resource is
     *                 not acquired.
     */
    abstract void setAcquired(boolean acquired);

    /**
     * Atomically acquires a free resource. But, due to race conditions, it may fail.
     *
     * @return <code>true</code> if acquire succeeded; <code>false</code> otherwise (may have
     * already been acquired by another thread).
     */
    abstract boolean acquire();

    /**
     * Atomically sets this resource as free; usually when returning a resource to the pool. <p>
     * Before releasing the resource back into the pool, <code>{@link #clear()}</code> is invoked.
     *
     * @throws RuntimeException if this Resource was already free.
     */
    abstract void release();

    /**
     * Cleans the resource of its contents so it can be returned to the pool. <p> You may use the
     * {@link #isFromPool()} indication to decide of a special action before returning a resource to
     * the pool, or discard any actions on resources not returning to the pool.
     *
     * @see #release()
     */
    abstract void clear();

}