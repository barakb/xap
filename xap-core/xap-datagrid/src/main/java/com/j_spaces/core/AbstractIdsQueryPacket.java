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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.AbstractQueryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A base template packet for IdsQueryPacket and IdsMultiRoutingQueryPacket The template packet
 * contains an IDs array.
 *
 * @author idan
 * @since 7.1.1
 */
public abstract class AbstractIdsQueryPacket extends AbstractQueryPacket {

    private static final long serialVersionUID = 1L;
    protected String _className;
    protected Object[] _ids;
    protected int _version;
    private AbstractProjectionTemplate _projectionTemplate;

    /**
     * Default constructor required by Externalizable.
     */
    public AbstractIdsQueryPacket() {
    }

    public AbstractIdsQueryPacket(Object[] ids, int version, ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        super(typeDesc, resultType);
        _projectionTemplate = projectionTemplate;
        _className = typeDesc.getTypeName();
        _ids = ids;
        _version = version;
    }

    public abstract Object getRouting(int objectIndex);

    public String getTypeName() {
        return _className;
    }

    public void setClassName(String className) {
        _className = className;
    }

    public Object[] getIds() {
        return _ids;
    }

    public void setIds(Object[] ids) {
        _ids = ids;
    }

    public int getVersion() {
        return _version;
    }

    public void setVersion(int version) {
        _version = version;
    }

    @Override
    public Object getID() {
        return _ids;
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        this._projectionTemplate = projectionTemplate;
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in, version);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        deserialize(in, PlatformLogicalVersion.getLogicalVersion());
    }

    private final void deserialize(ObjectInput in, PlatformLogicalVersion version) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readRepetitiveString(in);
        _ids = IOUtils.readObjectArray(in);
        _version = in.readInt();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            _projectionTemplate = IOUtils.readObject(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out, PlatformLogicalVersion.getLogicalVersion());
    }


    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        serialize(out, version);
    }

    private final void serialize(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        IOUtils.writeRepetitiveString(out, _className);
        IOUtils.writeObjectArray(out, _ids);
        out.writeInt(_version);
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0)) // One more field and we should change it to use flags
            IOUtils.writeObject(out, _projectionTemplate);
    }

    @Override
    public boolean isIdsQuery() {
        return true;
    }
}
