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


package com.gigaspaces.cluster.replication.gateway.conflict;


/**
 * A base class for a gateway replication Sink component conflict resolver. An appropriate method is
 * invoked for the following conflicts: <p> 1. {@link DataConflict} - conflicts caused by
 * write/update/take operation execution.<br/> 2. {@link RegisterTypeDescriptorConflict} - conflict
 * caused by a type registration operation execution.<br/> 3. {@link AddIndexConflict} - conflict
 * caused by an add indexes operation execution.<br/> </p> <p> The conflict resolver will be called
 * if there is some conflict when attempting to apply the replication affect into the local space.
 * If the cause for the conflict is {@link EntryLockedUnderTransactionConflict}, there will be an
 * automatic retry of the operation according to predefined configuration. In such case the conflict
 * resolver is invoked only after the amount of configurable operation retries has exceeded and the
 * {@link EntryLockedUnderTransactionConflict} conflict still occurs. </p>
 *
 * @author eitany
 * @since 8.0.3
 */
public abstract class ConflictResolver {
    /**
     * Invoked after a {@link DataConflict} occurred.
     *
     * The default behavior of the conflict resolver is to abort all the operation that exists
     * inside the provided {@link DataConflict}.
     *
     * @param sourceGatewayName The source gateway name the operations were replicated from.
     * @param conflict          The {@link DataConflict} instance representing the conflict.
     */
    public void onDataConflict(String sourceGatewayName, DataConflict conflict) {
        conflict.abortAll();
    }

    /**
     * Invoked after a {@link RegisterTypeDescriptorConflict} occurred.
     *
     * @param sourceGatewayName The source gateway name the operation was replicated from.
     * @param conflict          The {@link RegisterTypeDescriptorConflict} instance representing the
     *                          conflict.
     */
    public void onRegisterTypeDescriptorConflict(String sourceGatewayName, RegisterTypeDescriptorConflict conflict) {
    }

    /**
     * Invoked after an {@link AddIndexConflict} occurred.
     *
     * @param sourceGatewayName The source gateway name the operation was replicated from.
     * @param conflict          The {@link AddIndexConflict} instance representing the conflict.
     */
    public void onAddIndexConflict(String sourceGatewayName, AddIndexConflict conflict) {
    }

}
