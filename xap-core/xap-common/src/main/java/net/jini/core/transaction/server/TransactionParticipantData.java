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

package net.jini.core.transaction.server;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;

/**
 * Interface for jini transaction participant data.<br> Each transaction participant(space) sends
 * this info to mirror. <br> TransactionParticipantData contains information about transaction at
 * the time of commit - <br>transaction id, number of partitions that participated at this
 * transaction and the transaction participants id. <br>
 *
 * @author anna
 * @since 7.1
 * @deprecated since 9.0.1 - see {@link com.gigaspaces.transaction.TransactionParticipantMetaData}.
 */
@Deprecated
public interface TransactionParticipantData extends ISwapExternalizable {

    /**
     * The id of the space that committed the transaction.
     *
     * @return the participantId
     */
    public int getParticipantId();

    /**
     * Number of participants in transaction
     *
     * @return the participantsCount
     */
    public int getTransactionParticipantsCount();

    /**
     * The id of the distributed transaction
     *
     * @return the transactionId
     */
    public long getTransactionId();


}