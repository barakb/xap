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

package com.j_spaces.jms;

import net.jini.core.transaction.Transaction;

import java.util.LinkedList;

/**
 * Stores the context of an XA transaction.
 */
class XATrasactionContext {

    /**
     * The transaction branch is suspended.
     */
    static final int SUSPENDED = 1;

    /**
     * The transaction branch is active.
     */
    static final int SUCCESS = 2;

    /**
     * The transaction branch failed.
     */
    static final int ROLLBACK_ONLY = 3;

    /**
     * The transaction branch is prepared.
     */
    static final int PREPARED = 4;

    /**
     * The status of the transaction branch.
     */
    private int status = SUCCESS;

    /**
     * The associated transaction.
     */
    private Transaction transaction;

    /**
     * The list of unsent messages.
     */
    private LinkedList<GSMessageImpl> sentMessages;


    /**
     * Creates s a new instance of XATrasactionContext.
     *
     * @param transaction the associated transaction.
     */
    XATrasactionContext(Transaction transaction) {
        this.transaction = transaction;
        sentMessages = new LinkedList<GSMessageImpl>();
    }

    /**
     * Returns the status of the transaction branch.
     *
     * @return the status.
     */
    public int getStatus() {
        return status;
    }

    /**
     * Sets the status of the transaction branch.
     *
     * @param status the new status.
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Returns the associated transaction.
     *
     * @return the associated transaction.
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Sets the associated transaction.
     *
     * @param the associated transaction.
     */
    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Returns the messages list.
     *
     * @return the messages list.
     */
    public LinkedList<GSMessageImpl> getSentMessages() {
        return sentMessages;
    }

    /**
     * Sets the messages list.
     *
     * @param sentMessages the messages list.
     */
    public void setSentMessages(LinkedList<GSMessageImpl> sentMessages) {
        this.sentMessages = sentMessages;
    }
}
