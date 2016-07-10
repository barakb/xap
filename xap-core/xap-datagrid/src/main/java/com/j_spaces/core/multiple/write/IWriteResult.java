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

package com.j_spaces.core.multiple.write;

import com.j_spaces.core.LeaseContext;

import net.jini.core.lease.Lease;

import java.io.Serializable;


/**
 * Hold result that can be either lease or Throwable. Used with WriteMultiplePartialFailureException
 * to hold multiple results for multiple write operations.
 *
 * @author Barak
 */
public interface IWriteResult extends Serializable {

    public static enum ResultType {
        LEASE, ERROR
    }

    /**
     * @return the result type of this value, can be one of enum ResultType
     */
    public ResultType getResultType();

    /**
     * @return the error value that this value represent, or null if getResultType() does not return
     * ERROR.
     */
    public Throwable getError();

    /**
     * @return the lease that this value stands for , or null if getResultType() does not return
     * LEASE.
     * @deprecated Since 8.0.3 - Use {@link #getLeaseContext()} instead.
     */
    @Deprecated
    public Lease getLease();

    /**
     * @return the lease that this value stands for , or null if getResultType() does not return
     * LEASE.
     */
    public LeaseContext<?> getLeaseContext();

    /**
     * @return true if and only if this write result contains error value.
     * @since 6.6.1
     */
    public boolean isError();

}