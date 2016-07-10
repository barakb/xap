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

import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import net.jini.core.lease.Lease;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * the entry packet used to move {@link Externalizable} object from the proxy to the server.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ExternalizableEntryPacket extends AbstractEntryPacket {
    private static final long serialVersionUID = 1L;

    private static final int NULL_VERSION = Integer.MIN_VALUE;
    private static final long NULL_TTL = Long.MIN_VALUE;

    private Externalizable _object;
    private Object[] _fixedProperties;
    private Map<String, Object> _dynamicProperties;
    private String _uid;
    private int _version = NULL_VERSION;
    private long _timeToLive = NULL_TTL;
    private boolean _fifo;
    private Boolean _transient;
    private boolean _noWriteLease;
    private boolean _returnOnlyUIDs;
    private ICustomQuery _customQuery;

    private transient boolean _resetWhenWired;

    /**
     * Default constructor required by {@link java.io.Externalizable}.
     */
    public ExternalizableEntryPacket() {
    }

    public ExternalizableEntryPacket(ITypeDesc typeDesc, EntryType entryType, Externalizable object) {
        super(typeDesc, entryType);
        this._object = object;
    }

    public ExternalizableEntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties,
                                     Map<String, Object> dynamicProperties, String uid, int version, long timeToLive, boolean isTransient) {
        super(typeDesc, entryType);

        setFieldsValues(fixedProperties);
        setDynamicProperties(dynamicProperties);
        setUID(uid);
        setVersion(version);
        setTransient(isTransient);
        setTTL(timeToLive);
        _fifo = false;
        _noWriteLease = false;

        // if object was created successfully attributes that should be held by the object
        // itself should be cleaned from the entryPacket fields. it is achieved using the
        // setters side effect of cleaning the field if the value can be stored by the object itself.        
        try {
            _object = (Externalizable) toObject(_entryType);
        } catch (ConversionException e) {
            throw new ProxyInternalSpaceException("Unable to to create new object ot type: [" + _typeDesc.getTypeName() + "]", e);
        }
        _resetWhenWired = true;
    }

    public TransportPacketType getPacketType() {
        return TransportPacketType.ENTRY_PACKET;
    }

    public String getTypeName() {
        if (_typeDesc != null)
            return _typeDesc.getTypeName();
        else if (_object != null)
            return _object.getClass().getName();

        return null;
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        if (_object != null && entryType == _entryType)
            return _object;

        return super.toObject(entryType, storageTypeDeserialization);
    }

    public Object[] getFieldValues() {
        if (_object != null)
            return getIntrospector().getSerializedValues(_object);

        return _fixedProperties;
    }

    public void setFieldsValues(Object[] values) {
        if (values == null || values.length == 0)
            return;

        if (_object == null)
            _fixedProperties = values;
        else {
            getIntrospector().setValues(_object, values);
            // MUST clean the temporary storage so that getValues() will retrieve the values from the right place.
            _fixedProperties = null;
        }
    }

    public Map<String, Object> getDynamicProperties() {
        if (_object != null)
            return getIntrospector().getDynamicProperties(_object);

        return _dynamicProperties;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        if (dynamicProperties == null || dynamicProperties.isEmpty())
            return;

        if (_object == null)
            _dynamicProperties = dynamicProperties;
        else {
            getIntrospector().setDynamicProperties(_object, dynamicProperties);
            // MUST clean the temporary storage so that getDynamicProperties() will retrieve the values from the right place.
            _dynamicProperties = null;
        }
    }

    public Object getFieldValue(int index) {
        if (_object != null)
            return getIntrospector().getValue(_object, index);

        return _fixedProperties != null ? _fixedProperties[index] : null;
    }

    public void setFieldValue(int index, Object value) {
        getIntrospector().setValue(_object, value, index);
    }

    public String getUID() {
        if (_uid != null)
            return _uid;

        return (_object != null ? getIntrospector().getUID(_object, isTemplate(), true) : null);
    }

    public void setUID(String uid) {
        _uid = uid;
        if (_object != null)
            getIntrospector().setUID(_object, uid);
    }

    public int getVersion() {
        /*
         * part of GS-4165. TODO remove when #createFullObjectIfNeeded doesn't
         * call converter.toPojo(this) when this._object hasn't yet been
         * initialized.
         * 
         * fixes NPE thrown from:
         * com.gigaspaces.test.version.ValidateEntryVersionConflictException
         * /./qaSpace?versioned embedded POJO_EXTERNALIZABLE
         */
        if (_version != NULL_VERSION)
            return _version;

        return (_object != null ? getIntrospector().getVersion(_object) : 0);
    }

    public void setVersion(int version) {
        if (_object == null || !getIntrospector().setVersion(_object, version))
            _version = version;
        else
            _version = NULL_VERSION;
    }

    public long getTTL() {
        if (_timeToLive != NULL_TTL)
            return _timeToLive;

        return (_object != null ? getIntrospector().getTimeToLive(_object) : Lease.FOREVER);
    }

    public void setTTL(long ttl) {
        if (_object == null || !getIntrospector().setTimeToLive(_object, ttl))
            _timeToLive = ttl;
        else
            _timeToLive = NULL_TTL;
    }

    public boolean isFifo() {
        return _fifo;
    }

    public boolean isTransient() {
        if (_transient != null)
            return _transient;

        return (_object != null ? getIntrospector().isTransient(_object) : false);
    }

    private void setTransient(boolean isTransient) {
        if (_object == null || !getIntrospector().setTransient(_object, isTransient))
            _transient = isTransient;
        else
            _transient = null;
    }

    public boolean isNoWriteLease() {
        return _noWriteLease;
    }

    public String[] getMultipleUIDs() {
        return null;
    }

    public void setMultipleUIDs(String[] uids) {
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUIDs;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
        this._returnOnlyUIDs = returnOnlyUIDs;
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        _customQuery = customQuery;
    }


    protected boolean isTemplate() {
        return false;
    }

    private ITypeIntrospector<Object> getIntrospector() {
        return _typeDesc.getIntrospector(_entryType);
    }

    private void clearUneededSerializedData() {
        if (_resetWhenWired && _object != null && _typeDesc != null) {
            _fixedProperties = null;
            _dynamicProperties = null;
            if (getIntrospector().hasVersionProperty(_object))
                _version = NULL_VERSION;
            if (getIntrospector().hasTimeToLiveProperty(_object))
                _timeToLive = NULL_TTL;
            if (getIntrospector().hasTransientProperty(_object))
                _transient = null;
        }
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return false;
    }


    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        clearUneededSerializedData();
        super.writeExternal(out, version);
        serialize(out);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        clearUneededSerializedData();
        super.writeToSwap(out);
        serialize(out);
    }

    private final void serialize(ObjectOutput out) throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        out.writeObject(_object);

        //don't spend time if nothing to send
        if (flags == 0)
            return;

        if (_uid != null)
            IOUtils.writeString(out, _uid);
        if (_version != NULL_VERSION)
            out.writeInt(_version);
        if (_timeToLive != NULL_TTL)
            out.writeLong(_timeToLive);
        if (_fifo)
            out.writeBoolean(_fifo);
        if (_transient != null)
            out.writeBoolean(_transient);
        if (_noWriteLease)
            out.writeBoolean(_noWriteLease);
        if (_customQuery != null)
            IOUtils.writeObject(out, _customQuery);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        deserialize(in);
    }


    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final short flags = in.readShort();
        _object = (Externalizable) in.readObject();

        //don't spend time if no flags were sent
        if (flags == 0)
            return;

        if ((flags & FLAG_UID) != 0)
            _uid = IOUtils.readString(in);
        if ((flags & FLAG_VERSION) != 0)
            _version = in.readInt();
        if ((flags & FLAG_TIME_TO_LIVE) != 0)
            _timeToLive = in.readLong();
        if ((flags & FLAG_FIFO) != 0)
            _fifo = in.readBoolean();
        if ((flags & FLAG_TRNASIENT) != 0)
            _transient = in.readBoolean();
        if ((flags & FLAG_NO_WRITE_LEASE) != 0)
            _noWriteLease = in.readBoolean();
        this._returnOnlyUIDs = (flags & FLAG_RETURN_ONLY_UIDS) != 0;
        if ((flags & FLAG_CUSTOM_QUERY) != 0)
            _customQuery = IOUtils.readObject(in);
    }

    private static final short FLAG_UID = 1 << 0;
    private static final short FLAG_VERSION = 1 << 1;
    private static final short FLAG_TIME_TO_LIVE = 1 << 2;
    private static final short FLAG_FIFO = 1 << 3;
    private static final short FLAG_TRNASIENT = 1 << 4;
    private static final short FLAG_NO_WRITE_LEASE = 1 << 5;
    private static final short FLAG_RETURN_ONLY_UIDS = 1 << 6;
    private static final short FLAG_CUSTOM_QUERY = 1 << 7;

    private short buildFlags() {
        short flags = 0;

        if (_uid != null)
            flags |= FLAG_UID;
        if (_version != NULL_VERSION)
            flags |= FLAG_VERSION;
        if (_timeToLive != NULL_TTL)
            flags |= FLAG_TIME_TO_LIVE;
        if (_fifo)
            flags |= FLAG_FIFO;
        if (_transient != null)
            flags |= FLAG_TRNASIENT;
        if (_noWriteLease)
            flags |= FLAG_NO_WRITE_LEASE;
        if (_returnOnlyUIDs)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_customQuery != null)
            flags |= FLAG_CUSTOM_QUERY;

        return flags;
    }

    @Override
    public void toText(Textualizer textualizer) {
        if (_typeDesc != null && getIntrospector() != null)
            super.toText(textualizer);
        else {
            textualizer.append("typeName", getTypeName());
            textualizer.append("uid", _uid);
            textualizer.append("version", _version);
            textualizer.append("object", _object);
        }
    }

    @Override
    public boolean isExternalizableEntryPacket() {
        return true;
    }
}
