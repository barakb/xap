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

package com.gigaspaces.sync;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

/**
 * Represents a single data operation
 *
 * @author eitany
 * @since 9.0.1
 */
public interface DataSyncOperation {

    /**
     * @return The operation's data Id.
     * @since 9.5
     */
    Object getSpaceId();

    /**
     * @return The operation's data UID.
     */
    String getUid();

    /**
     * @return the operation type.
     */
    DataSyncOperationType getDataSyncOperationType();

    /**
     * @return the operation data as object (i.e pojo), this can only be used if {@link
     * #supportsDataAsObject()} return true, otherwise an exception will be thrown.
     */
    Object getDataAsObject();

    /**
     * @return the operation data as space document, this can only be used if {@link
     * #supportsDataAsDocument()} return true, otherwise an exception will be thrown.
     */
    SpaceDocument getDataAsDocument();

    /**
     * @return the type descriptor of the data type. this can only be used if {@link
     * #supportsGetTypeDescriptor()} return true, otherwise an exception will be thrown.
     */
    SpaceTypeDescriptor getTypeDescriptor();

    /**
     * @return whether this data operation support the {@link #getTypeDescriptor()} operation.
     */
    boolean supportsGetTypeDescriptor();

    /**
     * @return whether this data operation support the {@link #getDataAsObject()} operation.
     */
    boolean supportsDataAsObject();

    /**
     * @return whether this data operation support the {@link #getDataAsDocument()} operation.
     */
    boolean supportsDataAsDocument();

    /**
     * @return whether this data operation support the {@link #getSpaceId()} operation.
     * @since 9.5
     */
    boolean supportsGetSpaceId();
}
