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


package org.openspaces.core;

import net.jini.core.transaction.TransactionException;

import org.springframework.dao.DataAccessException;

/**
 * An exception occurred during a space operation that has to do with transactional semantics. Wraps
 * {@link net.jini.core.transaction.TransactionException}.
 *
 * @author kimchy
 */
public class TransactionDataAccessException extends DataAccessException {

    private static final long serialVersionUID = -6113375689076743832L;

    public TransactionDataAccessException(String msg) {
        super(msg);
    }

    public TransactionDataAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TransactionDataAccessException(TransactionException e) {
        super(e.getMessage(), e);
    }

    public TransactionDataAccessException(org.springframework.transaction.TransactionException e) {
        super(e.getMessage(), e);
    }
}
