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

/**
 * Thrown when an operation is performed on an inactive transaction.
 *
 * @author kimchy
 */
public class InactiveTransactionException extends TransactionDataAccessException {

    private static final long serialVersionUID = -2143596398922984453L;

    public InactiveTransactionException(com.j_spaces.core.TransactionNotActiveException e) {
        super(e.getMessage(), e);
    }

    public InactiveTransactionException(TransactionException e) {
        super(e);
    }
}
