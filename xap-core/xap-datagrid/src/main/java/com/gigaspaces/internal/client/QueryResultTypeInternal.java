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

package com.gigaspaces.internal.client;

import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

/**
 * Indicates what type of result is expected when querying the space.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public enum QueryResultTypeInternal {
    NOT_SET(0, null),
    DOCUMENT_ENTRY(1, EntryType.DOCUMENT_JAVA),
    OBJECT_JAVA(2, EntryType.OBJECT_JAVA),
    EXTERNAL_ENTRY(4, EntryType.EXTERNAL_ENTRY),
    PBS_OLD(5, EntryType.DOCUMENT_JAVA),
    OBJECT_DOTNET(6, EntryType.OBJECT_DOTNET),
    DOCUMENT_DOTNET(7, EntryType.DOCUMENT_DOTNET),
    CPP(8, EntryType.CPP);

    private static final int MAX = 8;
    private static final QueryResultTypeInternal[] _values;

    private final byte _code;
    private final EntryType _entryType;

    static {
        _values = new QueryResultTypeInternal[MAX + 1];
        _values[NOT_SET._code] = NOT_SET;
        _values[DOCUMENT_ENTRY._code] = DOCUMENT_ENTRY;
        _values[OBJECT_JAVA._code] = OBJECT_JAVA;
        _values[3 /*Externalizable*/] = OBJECT_JAVA;
        _values[EXTERNAL_ENTRY._code] = EXTERNAL_ENTRY;
        _values[PBS_OLD._code] = PBS_OLD;
        _values[OBJECT_DOTNET._code] = OBJECT_DOTNET;
        _values[DOCUMENT_DOTNET._code] = DOCUMENT_DOTNET;
        _values[CPP._code] = CPP;
    }

    private QueryResultTypeInternal(int code, EntryType entryType) {
        this._code = (byte) code;
        this._entryType = entryType;
    }

    public byte getCode() {
        return _code;
    }

    public byte getCode(PlatformLogicalVersion version) {
        return _code;
    }

    public static QueryResultTypeInternal fromCode(byte code) {
        return _values[code];
    }

    public EntryType getEntryType() {
        return _entryType;
    }

    public boolean isPbs() {
        return this == OBJECT_DOTNET || this == DOCUMENT_DOTNET || this == CPP || this == PBS_OLD;
    }

    public static QueryResultTypeInternal fromEntryType(EntryType entryType) {
        if (entryType == null)
            return QueryResultTypeInternal.NOT_SET;
        switch (entryType) {
            case OBJECT_JAVA:
                return QueryResultTypeInternal.OBJECT_JAVA;
            case DOCUMENT_JAVA:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case CPP:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case OBJECT_DOTNET:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case DOCUMENT_DOTNET:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case EXTERNAL_ENTRY:
                return QueryResultTypeInternal.EXTERNAL_ENTRY;
            default:
                throw new IllegalArgumentException("Unsupported entry type: " + entryType);
        }
    }

    public static QueryResultTypeInternal getUpdateResultType(IEntryPacket packet) {
        EntryType entryType = packet.getEntryType();
        if (entryType == null)
            return QueryResultTypeInternal.NOT_SET;
        switch (entryType) {
            case OBJECT_JAVA:
                return QueryResultTypeInternal.OBJECT_JAVA;
            case DOCUMENT_JAVA:
                // Backwards compatibility protection: old (pre-9.0.2) PBS packets arrive with DOCUMENT type.
                if (packet.getPacketType() == TransportPacketType.PBS)
                    return QueryResultTypeInternal.PBS_OLD;
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case OBJECT_DOTNET:
                return QueryResultTypeInternal.OBJECT_DOTNET;
            case DOCUMENT_DOTNET:
                return QueryResultTypeInternal.DOCUMENT_DOTNET;
            case CPP:
                return QueryResultTypeInternal.CPP;
            case EXTERNAL_ENTRY:
                // Backwards compatibility protection: old (pre-8.0.0) PBS packets arrive with ExternalEntry type.
                if (packet.getPacketType() == TransportPacketType.PBS)
                    return QueryResultTypeInternal.PBS_OLD;
                return QueryResultTypeInternal.EXTERNAL_ENTRY;
            default:
                throw new IllegalArgumentException("Unsupported entry type: " + entryType);
        }
    }
}
