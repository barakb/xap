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


package org.openspaces.core.map;

import net.jini.core.transaction.Transaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A lock handle allowing to perform map operations when the key is locked. Internally holds the
 * transaction representing the lock and it can be accessed to be passed to any operation that needs
 * to be performed under the same lock.
 *
 * <p>With {@link org.openspaces.core.GigaMap} this is done automatically by just passing the lock
 * handle to an operation.
 *
 * @author kimchy
 */
public class LockHandle implements Externalizable {

    private static final long serialVersionUID = -8380130385514899614L;

    private LockManager lockManager;

    private Transaction tx;

    private Object key;

    public LockHandle() {

    }

    /**
     * Constructs a new Lock Handle
     */
    LockHandle(LockManager lockManager, Transaction tx, Object key) {
        this.lockManager = lockManager;
        this.tx = tx;
        this.key = key;
    }

    /**
     * Unlocks the given key.
     */
    public void unlock() {
        lockManager.unlock(key);
    }

    /**
     * Returns the transaction representing the lock on the given key.
     */
    public Transaction getTransaction() {
        return tx;
    }

    /**
     * Returns the key that is locked.
     */
    public Object getKey() {
        return key;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
        out.writeObject(key);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (Transaction) in.readObject();
        key = in.readObject();
    }
}
