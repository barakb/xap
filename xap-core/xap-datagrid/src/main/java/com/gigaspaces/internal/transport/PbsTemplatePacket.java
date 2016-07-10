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
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * PbsTemplatePacket defines the template object for Pbs layer. Adds template functionality to the
 * PbsEntryPacket super class
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class PbsTemplatePacket extends PbsEntryPacket implements ITemplatePacket {
    private static final long serialVersionUID = 1L;

    private QueryResultTypeInternal _queryResultType;

    public static PbsTemplatePacket getNullTemplate(QueryResultTypeInternal queryResultType) {
        PbsTemplatePacket result = new PbsTemplatePacket(null, null, queryResultType);
        result._className = Object.class.getName();
        result._fieldsValues = new Object[0];
        result._unmarshContentState = CONTENT_ALL;

        return result;
    }

    public PbsTemplatePacket() {
        super();
    }

    public PbsTemplatePacket(ITypeDesc typeDesc, QueryResultTypeInternal queryResultType) {
        super(typeDesc, queryResultType.getEntryType());
        this._queryResultType = queryResultType;
    }

    public PbsTemplatePacket(byte[] byteStream, Map<String, Object> dynamicProperties, QueryResultTypeInternal queryResultType) {
        super(byteStream, dynamicProperties, queryResultType.getEntryType());
        this._queryResultType = queryResultType;
    }

    public QueryResultTypeInternal getQueryResultType() {
        return _queryResultType;
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

    @Override
    public PbsTemplatePacket clone() {
        return (PbsTemplatePacket) super.clone();
    }


    @Override
    public void validate() {
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
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in);
    }

    private void serialize(ObjectOutput out) throws IOException {
        out.writeByte(_queryResultType.getCode());
    }

    private void deserialize(ObjectInput in) throws IOException, ClassNotFoundException {
        _queryResultType = QueryResultTypeInternal.fromCode(in.readByte());
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return null;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
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
        return true;
    }

    @Override
    public boolean isAllIndexValuesSqlQuery() {
        return false;
    }
}
