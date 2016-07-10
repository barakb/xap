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

package org.openspaces.persistency.patterns;

import com.gigaspaces.sync.AddIndexData;
import com.gigaspaces.sync.ConsolidationParticipantData;
import com.gigaspaces.sync.IntroduceTypeData;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.TransactionData;

/**
 * An exception handler that delegates {@link com.gigaspaces.sync.SpaceSynchronizationEndpoint}
 * execution and calls the provided {@link org.openspaces.persistency.patterns.PersistencyExceptionHandler}
 * in case of exceptions on synchronization methods.
 *
 * @author eitany
 * @since 9.5
 */
public class SpaceSynchronizationEndpointExceptionHandler extends SpaceSynchronizationEndpoint {

    private final SpaceSynchronizationEndpoint interceptor;
    private final PersistencyExceptionHandler exceptionHandler;

    public SpaceSynchronizationEndpointExceptionHandler(SpaceSynchronizationEndpoint interceptor, PersistencyExceptionHandler exceptionHandler) {
        this.interceptor = interceptor;
        this.exceptionHandler = exceptionHandler;
    }

    public void onTransactionConsolidationFailure(ConsolidationParticipantData participantData) {
        interceptor.onTransactionConsolidationFailure(participantData);
    }

    public void onTransactionSynchronization(TransactionData transactionData) {
        try {
            interceptor.onTransactionSynchronization(transactionData);
        } catch (Exception e) {
            exceptionHandler.onException(e, transactionData);
        }
    }

    public void afterTransactionSynchronization(TransactionData transactionData) {
        interceptor.afterTransactionSynchronization(transactionData);
    }

    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        try {
            interceptor.onOperationsBatchSynchronization(batchData);
        } catch (Exception e) {
            exceptionHandler.onException(e, batchData);
        }
    }

    public void afterOperationsBatchSynchronization(OperationsBatchData batchData) {
        interceptor.afterOperationsBatchSynchronization(batchData);
    }

    public void onAddIndex(AddIndexData addIndexData) {
        try {
            interceptor.onAddIndex(addIndexData);
        } catch (Exception e) {
            exceptionHandler.onException(e, addIndexData);
        }
    }

    public void onIntroduceType(IntroduceTypeData introduceTypeData) {
        try {
            interceptor.onIntroduceType(introduceTypeData);
        } catch (Exception e) {
            exceptionHandler.onException(e, introduceTypeData);
        }
    }

}
