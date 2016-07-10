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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.client.ExternalEntry;

import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

public abstract class AbstractQueryPacket extends AbstractEntryPacket implements ITemplatePacket {
    private static final long serialVersionUID = 1L;

    private QueryResultTypeInternal _queryResultType;

    protected AbstractQueryPacket() {
    }

    protected AbstractQueryPacket(ITypeDesc typeDesc, QueryResultTypeInternal resultType) {
        super(typeDesc, resultType.getEntryType());
        this._queryResultType = resultType;
    }

    public TransportPacketType getPacketType() {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    public Map<String, Object> getDynamicProperties() {
        return null;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {

    }

    //--- ITemplatePacket properties

    public QueryResultTypeInternal getQueryResultType() {
        return _queryResultType;
    }

    public void setQueryResultType(QueryResultTypeInternal queryResultType) {
        _queryResultType = queryResultType;
    }

    public boolean supportExtendedMatching() {
        return false;
    }

    public short[] getExtendedMatchCodes() {
        return null;
    }

    public Object[] getRangeValues() {
        return null;
    }

    public boolean[] getRangeValuesInclusion() {
        return null;
    }

    //--- IEntryPacket properties
    public String getTypeName() {
        return _typeDesc != null ? _typeDesc.getTypeName() : null;
    }

    public String[] getMultipleUIDs() {
        return null;
    }

    public void setMultipleUIDs(String[] uids) {
    }

    public Object[] getFieldValues() {
        return null;
    }

    public void setFieldsValues(Object[] values) {
    }

    public Object getFieldValue(int index) {
        Object[] values = getFieldValues();
        if (values == null)
            return null;

        return values[index];
    }

    public void setFieldValue(int index, Object value) {
        Object[] values = getFieldValues();
        if (values == null)
            return;

        values[index] = value;
    }

    @Override
    public Object getID() {
        int index = getIdentifierFieldIndex();
        return (index >= 0 ? getFieldValue(index) : null);
    }

    protected int getIdentifierFieldIndex() {
        return (_typeDesc != null ? _typeDesc.getIdentifierPropertyId() : -1);
    }

    public boolean isFifo() {
        return false;
    }

    public boolean isNoWriteLease() {
        return false;
    }

    public boolean isReturnOnlyUids() {
        return false;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
    }

    public boolean isTransient() {
        ITypeDesc typeDesc = getTypeDescriptor();
        return typeDesc.getIntrospector(getEntryType()).isTransient(null);
    }

    public long getTTL() {
        return 0;
    }

    public void setTTL(long ttl) {
    }

    public String getUID() {
        return null;
    }

    public void setUID(String uid) {
    }

    public int getVersion() {
        return 0;
    }

    public void setVersion(int version) {
    }

    public ICustomQuery getCustomQuery() {
        return null;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return null;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
    }

    @Override
    public AbstractQueryPacket clone() {
        return (AbstractQueryPacket) super.clone();
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        if (!isReturnOnlyUids())
            throw new InternalSpaceException("toObject() is not supported in query packets when isReturnOnlyUids() is false.");

        if (getMultipleUIDs() != null)
            return new ExternalEntry(getMultipleUIDs());
        return new ExternalEntry(getUID());
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readFromSwap(in);
        deserialize(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        serialize(out);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in);
    }

    private void serialize(ObjectOutput out)
            throws IOException {
        out.writeByte(_queryResultType.getCode());
    }

    private void deserialize(ObjectInput in)
            throws IOException {
        _queryResultType = QueryResultTypeInternal.fromCode(in.readByte());
    }

    @Override
    public void validate() {
    }

    @Override
    public boolean isIdQuery() {
        return false;
    }

    @Override
    public boolean isIdsQuery() {
        return false;
    }

    @Override
    public boolean isTemplateQuery() {
        return false;
    }

    @Override
    public boolean isAllIndexValuesSqlQuery() {
        return false;
    }

}
