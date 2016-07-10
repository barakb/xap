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
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A template packet used by readByIds operation. The template packet contains an IDs array and a
 * single routing object.
 *
 * @author idan
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class IdsMultiRoutingQueryPacket extends AbstractIdsQueryPacket {

    private static final long serialVersionUID = 1L;
    protected Object[] _routings;

    /**
     * Default constructor required by Externalizable.
     */
    public IdsMultiRoutingQueryPacket() {
        super();
    }

    public IdsMultiRoutingQueryPacket(Object[] ids, Object[] routings, int version,
                                      ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        super(ids, version, typeDesc, resultType, projectionTemplate);
        _routings = routings;
    }

    @Override
    public Object getRouting(int objectIndex) {
        return _routings[objectIndex];
    }

    @Override
    public Object getRoutingFieldValue() {
        return null;
    }

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

    public void set_version(int version) {
        _version = version;
    }

    @Override
    public Object getID() {
        return _ids;
    }

    public Object[] getRoutings() {
        return _routings;
    }

    @Override
    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _routings = IOUtils.readObjectArray(in);
    }

    @Override
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        serialize(out);
    }

    private final void serialize(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArray(out, _routings);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        deserialize(in);
    }


}
