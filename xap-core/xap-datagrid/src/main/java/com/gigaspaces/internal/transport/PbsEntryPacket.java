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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.pbs.PbsEntryFormatter;
import com.gigaspaces.serialization.pbs.PbsInputStream;
import com.gigaspaces.serialization.pbs.PbsOutputStream;
import com.gigaspaces.serialization.pbs.PbsStreamResource;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class PbsEntryPacket extends AbstractEntryPacket {
    private static final long serialVersionUID = 1L;

    protected static final byte CONTENT_NONE = 0;
    protected static final byte CONTENT_HEADER = 1;
    protected static final byte CONTENT_ALL = 2;

    protected String _className;
    private String _uid;
    private int _version;
    private long _timeToLive;
    private boolean _fifo;
    private boolean _transient;
    private boolean _noWriteLease;
    protected Object[] _fieldsValues;
    private String[] _multipleUIDs;
    private boolean _returnOnlyUIDs;

    private byte[] _marshedContent;
    protected byte _unmarshContentState;
    private int _unmarshedContentLength = 0;
    protected Object _routingFieldValue;

    private Map<String, Object> _dynamicProperties;
    private ICustomQuery _customQuery;

    //If dirty header is true, we assume the header have been unmarshed already
    private boolean _dirtyHeader;

    public PbsEntryPacket() {
    }

    public PbsEntryPacket(ITypeDesc typeDesc, EntryType entryType) {
        super(typeDesc, entryType);
        this._className = typeDesc != null ? typeDesc.getTypeName() : null;
    }

    public PbsEntryPacket(PbsInputStream input, ITypeDesc typeDesc, EntryType entryType) {
        this(typeDesc, entryType);
        readHeader(input);
        readFixedProperties(input);
    }

    public PbsEntryPacket(byte[] byteStream, Map<String, Object> dynamicProperties, EntryType entryType) {
        this(null, entryType);
        this._marshedContent = byteStream;
        this._dynamicProperties = dynamicProperties;
        this._unmarshContentState = CONTENT_NONE;
    }

    public PbsEntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties, Map<String, Object> dynamicProperties,
                          String uid, int version, long timeToLive, boolean isTransient) {
        this(typeDesc, entryType);

        this._uid = uid;
        this._fieldsValues = fixedProperties;
        this._dynamicProperties = dynamicProperties;
        this._version = version;
        this._timeToLive = timeToLive;
        this._transient = isTransient;
        this._noWriteLease = false;
        this._fifo = false;

        this._unmarshContentState = CONTENT_ALL;
    }

    @Override
    public IEntryPacket clone() {
        // Get fields values:
        // NOTE: this.getFieldValues() is invoked before super.clone() because it might affect other members.
        final Object[] fieldsValues = this.getFieldValues();
        // Clone super:
        IEntryPacket packet = super.clone();
        // Clone values:
        if (fieldsValues != null)
            packet.setFieldsValues(fieldsValues.clone());
        // Return cloned packet.
        return packet;
    }

    public byte[] getStreamBytes() {
        marsh();
        return this._marshedContent;
    }

    public TransportPacketType getPacketType() {
        return TransportPacketType.PBS;
    }

    public String[] getMultipleUIDs() {
        return _multipleUIDs;
    }

    public void setMultipleUIDs(String[] multipleUIDs) {
        this._multipleUIDs = multipleUIDs;
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUIDs;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
        this._returnOnlyUIDs = returnOnlyUIDs;
    }

    public long getTTL() {
        return _timeToLive;
    }

    public void setTTL(long ttl) {
        this._timeToLive = ttl;
    }

    public boolean isNoWriteLease() {
        return _noWriteLease;
    }

    public String getTypeName() {
        unmarsh(CONTENT_HEADER);
        return _className;
    }

    public String getUID() {
        unmarsh(CONTENT_HEADER);
        return _uid;
    }

    public void setUID(String uid) {
        _dirtyHeader = true;
        this._uid = uid;
    }

    public int getVersion() {
        unmarsh(CONTENT_HEADER);
        return _version;
    }

    public void setVersion(int version) {
        if (getVersion() != version) {
            _dirtyHeader = true;
            this._version = version;
        }
    }

    public boolean isFifo() {
        unmarsh(CONTENT_HEADER);
        return _fifo;
    }

    public boolean isTransient() {
        unmarsh(CONTENT_HEADER);
        return _transient;
    }

    public Object getRoutingFieldValue() {
        if (_typeDesc.isAutoGenerateRouting())
            return SpaceUidFactory.extractPartitionId(getUID());

        unmarsh(CONTENT_HEADER);

        if (_routingFieldValue == null && _typeDesc.getRoutingPropertyId() != -1) {
            unmarsh(CONTENT_ALL);
            return getFieldValue(_typeDesc.getRoutingPropertyId());
        }

        return _routingFieldValue;
    }

    public Object[] getFieldValues() {
        unmarsh(CONTENT_ALL);
        return _fieldsValues;
    }

    public void setFieldsValues(Object[] values) {
        _dirtyHeader = true;
        this._fieldsValues = values;
    }

    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    public Object getFieldValue(int index) {
        return getFieldValues()[index];
    }

    public void setFieldValue(int index, Object value) {
        try {
            _fieldsValues[index] = value;
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    @Override
    public boolean supportsTypeDescChecksum() {
        return false;
    }

    @Override
    public int getTypeDescChecksum() {
        throw new IllegalStateException();
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        _customQuery = customQuery;
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return true;
    }

    private void marsh() {
        if (_dirtyHeader) {
            unmarsh(CONTENT_ALL);
            this._marshedContent = null;
            this._dirtyHeader = false;
        }

        if (this._marshedContent == null) {
            // Initialize output stream:
            PbsOutputStream output = PbsStreamResource.getOutputStream();
            try {
                // Write entry to stream:
                PbsEntryFormatter.writeEntry(output, _fieldsValues, _className,
                        _uid, _version, _transient, _fifo, false);
                // Get content from stream:
                this._marshedContent = output.toByteArray();
            } finally {
                //Release the output stream
                PbsStreamResource.releasePbsStream(output);
            }
        }
    }

    private void unmarsh(byte requiredContent) {
        if ((this._unmarshContentState < requiredContent) && _marshedContent != null) {
            // Initialize input stream:
            PbsInputStream input = PbsStreamResource.getInputStream(_marshedContent);
            try {
                // Get stream length:
                int totalContentLength = input.available();
                // Skip previously read content, if any:
                if (this._unmarshedContentLength > 0)
                    input.skip(this._unmarshedContentLength);

                // If header is not unmarshed yet but required, unmarsh it:
                if (this._unmarshContentState < CONTENT_HEADER && requiredContent >= CONTENT_HEADER) {
                    // Unmarsh header:
                    readHeader(input);
                    // Update unmarshed content state:
                    this._unmarshContentState = CONTENT_HEADER;
                    // Update unmarshed content length:
                    this._unmarshedContentLength = totalContentLength - input.available();
                    // If no more content required, stop reading and return:
                    if (requiredContent == CONTENT_HEADER)
                        return;
                }

                // If the remaining content is not unmarshed yet but required, unmarsh it:
                if (this._unmarshContentState < CONTENT_ALL && requiredContent >= CONTENT_ALL) {
                    // Unmarsh fields values:
                    readFixedProperties(input);
                    // Update unmarshed content state:
                    _unmarshContentState = CONTENT_ALL;
                    // Update unmarshed content length:
                    this._unmarshedContentLength = totalContentLength - input.available();
                }
            } finally {
                PbsStreamResource.releasePbsStream(input);
            }
        }
    }

    private void readHeader(PbsInputStream input) {
        // Read Stream Version (currently ignored).
        short pbsVersion = input.readShort();
        // Read ClassName:
        this._className = input.readString();
        // Read UID:
        String uid = input.readString();
        this._uid = uid != null && uid.length() == 0 ? null : uid;
        // Read Version:
        int version = input.readInt();
        this._version = version == -1 ? 0 : version;
        // Read flags:
        int flags = input.readInt();
        boolean hasMetadata = ((flags & PbsEntryFormatter.BITMAP_METADATA) != 0);
        this._transient = ((flags & PbsEntryFormatter.BITMAP_TRANSIENT) != 0);
        this._fifo = ((flags & PbsEntryFormatter.BITMAP_FIFO) != 0);
        //boolean isReplicable = ((flags & PbsEntryFormatter.BITMAP_REPLICATE) != 0);

        // Read metadata, if relevant:
        if (hasMetadata)
            _typeDesc = PbsEntryFormatter.readTypeDesc(input, _className, this.getEntryType(), flags);

        // Read routing field value:
        // Note: This field is only relevant when .Net/CPP sends requests to the client for routing.
        this._routingFieldValue = PbsEntryFormatter.readFieldValue(input);
    }

    private void readFixedProperties(PbsInputStream input) {
        int length = input.readInt();
        _fieldsValues = new Object[length];
        for (int i = 0; i < length; i++)
            _fieldsValues[i] = PbsEntryFormatter.readFieldValue(input);
    }


    private static final short FLAG_MARSHED_CONTENT = 1 << 0;
    private static final short FLAG_MULTIPLE_UIDS = 1 << 1;
    private static final short FLAG_TIME_TO_LIVE = 1 << 2;
    private static final short FLAG_NO_WRITE_LEASE = 1 << 3;
    private static final short FLAG_RETURN_ONLY_UIDS = 1 << 4;
    private static final short FLAG_DYNAMIC_PROPERTIES = 1 << 5;
    private static final short FLAG_CUSTOM_QUERY = 1 << 6;

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserializePacket(in);
    }

    private final void deserializePacket(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // Read flags:
        final short flags = in.readShort();

        if ((flags & FLAG_MARSHED_CONTENT) != 0)
            _marshedContent = IOUtils.readByteArray(in);
        if ((flags & FLAG_MULTIPLE_UIDS) != 0)
            _multipleUIDs = IOUtils.readStringArray(in);
        if ((flags & FLAG_TIME_TO_LIVE) != 0)
            _timeToLive = in.readLong();
        if ((flags & FLAG_DYNAMIC_PROPERTIES) != 0)
            _dynamicProperties = IOUtils.readObject(in);
        if ((flags & FLAG_CUSTOM_QUERY) != 0)
            _customQuery = IOUtils.readObject(in);

        // Set boolean fields from flags:
        this._noWriteLease = (flags & FLAG_NO_WRITE_LEASE) != 0;
        this._returnOnlyUIDs = (flags & FLAG_RETURN_ONLY_UIDS) != 0;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        serializePacket(out, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserializePacket(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        serializePacket(out, version);
    }

    private final void serializePacket(ObjectOutput out,
                                       PlatformLogicalVersion version) throws IOException {
        marsh();

        // Calculate flags:
        final short flags = buildFlags(version);
        // Write flags:
        out.writeShort(flags);

        if (_marshedContent != null)
            IOUtils.writeByteArray(out, _marshedContent);
        if (_multipleUIDs != null)
            IOUtils.writeStringArray(out, _multipleUIDs);
        if (_timeToLive != 0)
            out.writeLong(_timeToLive);
        if ((flags & FLAG_DYNAMIC_PROPERTIES) != 0)
            IOUtils.writeObject(out, _dynamicProperties);
        if ((flags & FLAG_CUSTOM_QUERY) != 0)
            IOUtils.writeObject(out, _customQuery);
    }

    private short buildFlags(PlatformLogicalVersion version) {
        short flags = 0;

        if (_marshedContent != null)
            flags |= FLAG_MARSHED_CONTENT;
        if (_multipleUIDs != null)
            flags |= FLAG_MULTIPLE_UIDS;
        if (_timeToLive != 0)
            flags |= FLAG_TIME_TO_LIVE;
        if (_noWriteLease)
            flags |= FLAG_NO_WRITE_LEASE;
        if (_returnOnlyUIDs)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_dynamicProperties != null)
            flags |= FLAG_DYNAMIC_PROPERTIES;
        if (_customQuery != null)
            flags |= FLAG_CUSTOM_QUERY;

        return flags;
    }
}
