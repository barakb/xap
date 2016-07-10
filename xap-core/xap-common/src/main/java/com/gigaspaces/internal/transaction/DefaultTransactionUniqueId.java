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

package com.gigaspaces.internal.transaction;

import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author idan
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class DefaultTransactionUniqueId extends AbstractTransactionUniqueId {
    private static final long serialVersionUID = 1L;

    private long _transactionId;

    /**
     * Externalizable.
     */
    public DefaultTransactionUniqueId() {
    }

    public DefaultTransactionUniqueId(Uuid transactionManagerId, long transactionId) {
        super(transactionManagerId);
        this._transactionId = transactionId;
    }

    @Override
    public Long getTransactionId() {
        return _transactionId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(_transactionId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _transactionId = in.readLong();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (_transactionId ^ (_transactionId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        DefaultTransactionUniqueId other = (DefaultTransactionUniqueId) obj;
        if (_transactionId != other._transactionId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Default[" + _transactionId + (getTransactionManagerId() != null ? "-" + getTransactionManagerId() : "")
                + "]";
    }

}
