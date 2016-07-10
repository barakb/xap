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

package com.j_spaces.core.client;

import java.io.Serializable;

/**
 * UnderTxnLockedObject holds the details on Objects that are locked by a specific transaction. this
 * object returned in a list from ITransactionDetailsProvider.getLockedObjects() it contains: UId -
 * to retrieve the locked Object. lockType 		- READ or WRITE operationType	- the operation that done
 * with this object under Txn (read/write/update..) className		- the locked Object class name
 *
 * @author Mordy Arnon Gigaspaces.com
 * @version 1.0
 * @since 5.2
 */
@com.gigaspaces.api.InternalApi
public class UnderTxnLockedObject implements Serializable {
    private static final long serialVersionUID = -2941506806743031862L;

    private final Object _uid;
    private final int _lockType;        // use <code> UnderTxnLockedObject </code> constants.
    private final int _operationType;// use <code> SpaceOperations </code> constants.
    private final String _className;

    /**
     * Lock Types:  READ lock
     */
    public static final int LOCK_TYPE_READ_LOCK = 1;
    /**
     * WRITE lock
     */
    public static final int LOCK_TYPE_WRITE_LOCK = 2;

    /**
     * @param UId
     * @param lockType
     * @param operationType
     * @param className
     */
    public UnderTxnLockedObject(Object UId, int lockType, int operationType, String className) {
        _uid = UId;
        _lockType = lockType;
        _operationType = operationType;
        _className = className;
    }

    /**
     * @return the class name of the locked object.
     */
    public String getClassName() {
        return _className;
    }

    /**
     * @return the type of the Lock: READ or WRITE
     */
    public int getLockType() {
        return _lockType;
    }

    /**
     * use <code> SpaceOperations </code> constants.
     *
     * @return the operation on the locked object
     */
    public int getOperationType() {
        return _operationType;
    }

    /**
     * @return the object UId to retrieve the Object itself only by dirty read because it is locked
     */
    public Object getUid() {
        return _uid;
    }
}
