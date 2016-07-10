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

/**
 *
 */
package com.j_spaces.sadapter.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.exception.internal.PersistentInternalSpaceException;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.RemoteException;

/**
 * {@link EntryPacketDataConverter} is a wrapper for the {@link IPojoToEntryConverter} . It calls
 * the converter with the expected data type.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class EntryPacketDataConverter implements IDataConverter<IEntryPacket> {
    public static enum DataType {
        REAL,
        VIRTUAL,
        DOCUMENT,
        RAW;

        public static DataType getDataType(Class type) {
            if (IEntryPacket.class.isAssignableFrom(type)) {
                return DataType.RAW;
            } else if (IGSEntry.class.isAssignableFrom(type)) {
                return DataType.VIRTUAL;
            } else if (SpaceDocument.class.isAssignableFrom(type)) {
                return DataType.DOCUMENT;
            } else {
                return DataType.REAL;
            }
        }

        public EntryType getEntryType() {
            if (this == REAL)
                return EntryType.OBJECT_JAVA;
            if (this == VIRTUAL)
                return EntryType.EXTERNAL_ENTRY;
            if (this == DOCUMENT)
                return EntryType.DOCUMENT_JAVA;

            return null;
        }
    }

    final private DataType _dataType;

    // Converter
    final protected SpaceTypeManager _typeManager;

    /**
     * @param typeDescRepository
     * @param type
     */
    public EntryPacketDataConverter(SpaceTypeManager typeDescRepository, Class type) {
        _typeManager = typeDescRepository;
        _dataType = DataType.getDataType(type);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.IConverter#toIGSEntry(java.lang.Object)
     */
    public IEntryPacket toInternal(Object obj)
            throws RemoteException, UnusableEntryException, UnknownTypeException {
        if (_dataType == DataType.RAW)
            return (IEntryPacket) obj;

        final ObjectType objectType = ObjectType.fromObject(obj);
        final ITypeDesc typeDesc = _typeManager.getTypeDescriptorByObject(obj, objectType).getTypeDesc();

        EntryType entryType;
        if (objectType == ObjectType.EXTERNAL_ENTRY)
            entryType = EntryType.EXTERNAL_ENTRY;
        else if (objectType == ObjectType.DOCUMENT)
            entryType = EntryType.DOCUMENT_JAVA;
        else
            entryType = EntryType.OBJECT_JAVA;

        return EntryPacketFactory.createFromObject(obj, typeDesc, entryType, false);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.IConverter#toObject(com.j_spaces.core.IGSEntry)
     */
    public Object toObject(IEntryPacket entryPacket) {
        if (_dataType == DataType.RAW)
            return entryPacket;

        final String className = entryPacket.getTypeName();
        if (className == null)
            return null;

        final EntryType entryType = _dataType.getEntryType();

        try {
            final ITypeDesc typeDesc = _typeManager.loadServerTypeDesc(entryPacket).getTypeDesc();
            return typeDesc.getIntrospector(entryType).toObject(entryPacket);
        } catch (Exception e) {
            throw new PersistentInternalSpaceException("Illegal object type descriptor. Object type - " + entryType + " , class - " + className, e);
        }
    }

    @Override
    public SpaceDocument toDocument(IEntryPacket entryPacket) {
        return (SpaceDocument) entryPacket.toObject(QueryResultTypeInternal.DOCUMENT_ENTRY, StorageTypeDeserialization.EAGER);
    }
}
