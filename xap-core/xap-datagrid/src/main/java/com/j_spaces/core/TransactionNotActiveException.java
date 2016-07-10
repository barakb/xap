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


package com.j_spaces.core;

import net.jini.core.transaction.server.ServerTransaction;

/**
 * A TransactionNotActiveException is thrown when some transaction related operation is performed
 * but the transaction is not active.
 */
@com.gigaspaces.api.InternalApi
public class TransactionNotActiveException extends Exception {
    private static final long serialVersionUID = 7121485628822903065L;

    public ServerTransaction m_Xtn;

    public TransactionNotActiveException(ServerTransaction Xtn) {
        m_Xtn = Xtn;
    }

    @Override
    public String toString() {
        return "Transaction not active: " + m_Xtn;
    }
}