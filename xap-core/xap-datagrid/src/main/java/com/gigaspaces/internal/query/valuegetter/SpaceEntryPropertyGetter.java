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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A space value getter for getting a space entry property value.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryPropertyGetter extends AbstractSpaceValueGetter<ServerEntry> {
    private static final long serialVersionUID = -2780950582604448258L;

    private int _propertyIndex;
    private String _propertyName;

    /**
     * Default constructor required by Externalizable.
     */
    public SpaceEntryPropertyGetter() {
    }

    /**
     * Creates a property value getter using the specified property index.
     *
     * @param propertyId Index of property.
     */
    public SpaceEntryPropertyGetter(int index) {
        this._propertyIndex = index;
    }

    /**
     * Creates a property value getter using the specified property name.
     *
     * @param name Name of property.
     */
    public SpaceEntryPropertyGetter(String name) {
        this._propertyName = name;
        this._propertyIndex = -1;
    }

    @Override
    public Object getValue(ServerEntry target) {
        if (_propertyIndex == -1)
            _propertyIndex = prepare(target.getSpaceTypeDescriptor());

        if (_propertyIndex == -1)
            return target.getPropertyValue(_propertyName);

        return target.getFixedPropertyValue(_propertyIndex);
    }

    public String getPropertyName() {
        return _propertyName;
    }

    private int prepare(SpaceTypeDescriptor typeDescriptor) {
        return typeDescriptor.getFixedPropertyPosition(_propertyName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        SpaceEntryPropertyGetter other = (SpaceEntryPropertyGetter) obj;
        if (this._propertyIndex != other._propertyIndex)
            return false;
        if (!ObjectUtils.equals(this._propertyName, other._propertyName))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._propertyIndex = in.readInt();
        this._propertyName = IOUtils.readString(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        out.writeInt(this._propertyIndex);
        IOUtils.writeString(out, this._propertyName);
    }


    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        readExternalImpl(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }
}
