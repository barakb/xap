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

package com.gigaspaces.internal.cluster.node.handlers;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.IReplicationNode;

/**
 * Handle incoming replication event of executed transaction {@link IReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationInTransactionHandler {
    /**
     * Handles one phase commit transaction (single participant)
     */
    void inTransaction(IReplicationInContext context, ITransactionInContext transactionContext) throws Exception;

    /**
     * Handle prepare stage of two phase committed transaction
     */
    void inTransactionPrepare(IReplicationInContext context, ITransactionInContext transactionContext) throws Exception;

    /**
     * Handle commit stage of two phase committed transaction
     */
    void inTransactionCommit(IReplicationInContext context, ITransactionInContext transactionContext) throws Exception;

    /**
     * Handle abort stage of two phase committed transaction
     */
    void inTransactionAbort(IReplicationInContext context, ITransactionInContext transactionContext) throws Exception;

}
