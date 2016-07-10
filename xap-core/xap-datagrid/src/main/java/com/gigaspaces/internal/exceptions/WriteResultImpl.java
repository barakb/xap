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

package com.gigaspaces.internal.exceptions;

import com.gigaspaces.client.WriteMultipleException.IWriteResult;
import com.j_spaces.core.LeaseContext;

import net.jini.core.lease.Lease;

@com.gigaspaces.api.InternalApi
public class WriteResultImpl implements IWriteResult {
    static final long serialVersionUID = 0L;

    private ResultType _type = ResultType.LEASE;
    private Throwable _error;
    private Lease _lease;

    public static WriteResultImpl createResult(Lease lease, Throwable error) {
        return error != null ? createErrorResult(error) : createLeaseResult(lease);
    }

    public static WriteResultImpl createErrorResult(Throwable e) {
        WriteResultImpl r = new WriteResultImpl();
        r.setError(e);
        r.setType(ResultType.ERROR);
        return r;
    }

    public static WriteResultImpl createLeaseResult(Lease lease) {
        WriteResultImpl r = new WriteResultImpl();
        r.setLease(lease);
        r.setType(ResultType.LEASE);
        return r;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.IResult#getType()
     */
    public ResultType getResultType() {
        return _type;
    }

    public boolean isError() {
        return _type == ResultType.ERROR;
    }

    public void setType(ResultType type) {
        this._type = type;
    }

    public Throwable getError() {
        return _error;
    }

    public void setError(Throwable error) {
        this._error = error;
        if (error != null)
            this.setType(ResultType.ERROR);
    }

    public Lease getLease() {
        return _lease;
    }

    public void setLease(Lease lease) {
        _lease = lease;
    }

    @Override
    public String toString() {
        if (isError())
            return String.valueOf(_error);
        return String.valueOf(_lease);
    }

    public LeaseContext<?> getLeaseContext() {
        return (LeaseContext<?>) _lease;
    }
}
