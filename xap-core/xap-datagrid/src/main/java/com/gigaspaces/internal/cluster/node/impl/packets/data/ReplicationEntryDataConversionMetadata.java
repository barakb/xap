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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationEntryDataConversionMetadata implements Cloneable {

    private IEntryData _entryData;
    private boolean _requiresFullEntryData;
    private boolean _requiresReliableAsyncFullContent;
    private boolean _requiresPrevValue;
    private boolean wasModifiedByFilter;
    private ReplicationSingleOperationType _operationType;
    private QueryResultTypeInternal _requiredResultType = QueryResultTypeInternal.NOT_SET;
    private AbstractProjectionTemplate _projectionTemplate;
    private short _flags;

    public ReplicationEntryDataConversionMetadata() {
    }

    public ReplicationEntryDataConversionMetadata(ReplicationSingleOperationType operationType) {
        _operationType = operationType;
    }

    protected void onModify() {
    }


    public ReplicationEntryDataConversionMetadata entryData(IEntryData entryData) {
        onModify();
        _entryData = entryData;
        return this;
    }

    public ReplicationEntryDataConversionMetadata requiresFullEntryData() {
        onModify();
        _requiresFullEntryData = true;
        return this;
    }

    public ReplicationEntryDataConversionMetadata requiresReliableAsyncFullContent() {
        onModify();
        _requiresReliableAsyncFullContent = true;
        return this;
    }

    public ReplicationEntryDataConversionMetadata requiresPrevValue() {
        onModify();
        _requiresPrevValue = true;
        return this;
    }

    public ReplicationEntryDataConversionMetadata modifiedByFilter() {
        onModify();
        wasModifiedByFilter = true;
        return this;
    }

    public ReplicationEntryDataConversionMetadata flags(short flags) {
        onModify();
        _flags = flags;
        return this;
    }

    public ReplicationEntryDataConversionMetadata resultType(QueryResultTypeInternal resultType) {
        onModify();
        _requiredResultType = resultType;
        return this;
    }

    public ReplicationEntryDataConversionMetadata projectionTemplate(
            AbstractProjectionTemplate projectionTemplate) {
        onModify();
        _projectionTemplate = projectionTemplate;
        return this;
    }


    public IEntryData getEntryData() {
        return _entryData;
    }

    public short getFlags() {
        return _flags;
    }

    public boolean isRequiresFullEntryData() {
        return _requiresFullEntryData;
    }

    public ReplicationSingleOperationType getConvertToOperationType() {
        return _operationType;
    }

    public boolean isRequiresReliableAsyncFullContent() {
        return _requiresReliableAsyncFullContent;
    }

    public boolean isRequiresPrevValue() {
        return _requiresPrevValue;
    }

    public boolean isSameOperationConversion() {
        return _operationType == null;
    }

    public boolean isModifiedByFilter() {
        return wasModifiedByFilter;
    }

    public QueryResultTypeInternal getRequiredQueryResultType() {
        return _requiredResultType;
    }

    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    @Override
    public ReplicationEntryDataConversionMetadata clone() {
        try {
            return (ReplicationEntryDataConversionMetadata) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError();
        }
    }

    @Override
    public String toString() {
        return "ReplicationEntryDataConversionMetadata [requiresFullEntryData()="
                + requiresFullEntryData()
                + ", requiresReliableAsyncFullContent()="
                + requiresReliableAsyncFullContent()
                + ", getEntryData()="
                + getEntryData()
                + ", getFlags()="
                + getFlags()
                + ", isRequiresFullEntryData()="
                + isRequiresFullEntryData()
                + ", getConvertToOperationType()="
                + getConvertToOperationType()
                + ", isRequiresReliableAsyncFullContent()="
                + isRequiresReliableAsyncFullContent()
                + ", isSameOperationConversion()="
                + isSameOperationConversion()
                + ", getRequiredQueryResultType()="
                + getRequiredQueryResultType()
                + ", getProjectionTemplate()="
                + getProjectionTemplate() + "]";
    }

}
