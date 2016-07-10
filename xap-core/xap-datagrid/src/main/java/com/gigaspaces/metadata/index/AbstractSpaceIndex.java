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

package com.gigaspaces.metadata.index;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.SpaceIndexTypeHelper;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.serialization.AbstractExternalizable;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class AbstractSpaceIndex extends AbstractExternalizable implements ISpaceIndex {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private String _name;
    private SpaceIndexType _indexType;
    private boolean _isUnique;

    public AbstractSpaceIndex() {
    }

    protected AbstractSpaceIndex(String indexName, SpaceIndexType indexType, boolean isUnique) {
        this._name = indexName;
        this._indexType = indexType;
        this._isUnique = isUnique;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public SpaceIndexType getIndexType() {
        return _indexType;
    }

    @Override
    public boolean isUnique() {
        return _isUnique;
    }

    @Override
    public void setUnique(boolean val) {
        _isUnique = val;
    }

    @Override
    public boolean isMultiValuePerEntryIndex() {
        return false;
    }

    @Override
    public MultiValuePerEntryIndexTypes getMultiValueIndexType() {
        throw new UnsupportedOperationException();
    }


    /**
     * @return true if its a compound index
     */
    @Override
    public boolean isCompoundIndex() {
        return false;
    }

    @Override
    public ISpaceCompoundIndexSegment[] getCompoundIndexSegments() {
        throw new UnsupportedOperationException();
    }

    public int getNumSegments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((_name == null) ? 0 : _name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractSpaceIndex other = (AbstractSpaceIndex) obj;
        if (_name == null) {
            if (other._name != null)
                return false;
        } else if (!_name.equals(other._name))
            return false;
        return true;
    }

    private static final byte FLAG_VERSION = 1 << 0;
    private static final byte FLAG_UNIQUE = 1 << 1;

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final byte flags = in.readByte();

        _isUnique = ((flags & FLAG_UNIQUE) != 0);

        if ((flags & FLAG_VERSION) != 0) {
            PlatformLogicalVersion version = (PlatformLogicalVersion) in.readObject();
        }

        _name = IOUtils.readString(in);
        _indexType = SpaceIndexTypeHelper.fromCode(in.readByte());
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        serialize(out);
    }

    private final void serialize(ObjectOutput out) throws IOException {
        final byte flags = buildFlags();
        out.writeByte(flags);

        if ((flags & FLAG_VERSION) != 0) {
            final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
            out.writeObject(version);
        }

        IOUtils.writeString(out, _name);
        out.writeByte(SpaceIndexTypeHelper.toCode(_indexType));
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    private byte buildFlags() {
        byte flags = 0;

        flags |= FLAG_VERSION;

        if (_isUnique)
            flags |= FLAG_UNIQUE;
        return flags;
    }

    /**
     * This method is used by custom indexes (custom, path & collection).
     *
     * @return The index value stored in the custom query's custom indexes.
     */
    protected Object getIndexValueFromCustomIndex(ServerEntry entry) {
        TemplateEntryData templateHolder = (TemplateEntryData) entry;
        if (templateHolder.getCustomIndexes() != null) {
            for (IQueryIndexScanner scanner : templateHolder.getCustomIndexes()) {
                if (getName().equals(scanner.getIndexName()))
                    return (scanner.supportsTemplateIndex()) ? scanner.getIndexValue() : null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "SpaceIndex[name=" + _name + ", type=" + _indexType + ", unique=" + _isUnique + "]";
    }
}
