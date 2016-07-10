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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.version.PlatformLogicalVersion;

public enum EntryType {
    OBJECT_JAVA((byte) 1, false),
    //ENTRY((byte)2, false),			// Removed in 8.0
    //EXTERNALIZABLE((byte)3, false),	// Removed in 8.0
    EXTERNAL_ENTRY((byte) 4, true),
    //PBS((byte)5, true); 				// Removed in 8.0
    DOCUMENT_JAVA((byte) 6, true),        // Since 8.0
    CPP((byte) 7, true),                    // Since 9.0.2
    OBJECT_DOTNET((byte) 8, true),        // Since 9.0.2
    DOCUMENT_DOTNET((byte) 9, true);        // Since 9.0.2

    public static final int MAX = 9;
    private static final EntryType[] _values;

    private final byte _code;
    private final boolean _isVirtual;

    private EntryType(byte code, boolean isVirtual) {
        _code = code;
        _isVirtual = isVirtual;
    }

    static {
        _values = new EntryType[MAX + 1];
        _values[OBJECT_JAVA._code] = OBJECT_JAVA;
        _values[2 /*ENTRY._code*/] = OBJECT_JAVA;        // ENTRY was removed in 8.0, this is kept for backwards compatibility.
        _values[3 /*EXTERNALIZABLE._code*/] = OBJECT_JAVA;        // EXTERNALIZABLE was removed in 8.0, this is kept for backwards compatibility.
        _values[EXTERNAL_ENTRY._code] = EXTERNAL_ENTRY;
        _values[5 /*PBS._code*/] = EXTERNAL_ENTRY;    // PBS was removed in 8.0, this is kept for backwards compatibility.
        _values[DOCUMENT_JAVA._code] = DOCUMENT_JAVA;    // Modified in 9.0.2 (was - DOCUMENT and/or any PBS)
        _values[CPP._code] = CPP;                // Since 9.0.2
        _values[OBJECT_DOTNET._code] = OBJECT_DOTNET;    // Since 9.0.2
        _values[DOCUMENT_DOTNET._code] = DOCUMENT_DOTNET;    // Since 9.0.2
    }

    public byte getTypeCode() {
        return this._code;
    }

    public byte getTypeCode(PlatformLogicalVersion version) {
        return this._code;
    }

    public static EntryType fromByte(byte typeCode) {
        return _values[typeCode];
    }

    public boolean isVirtual() {
        return _isVirtual;
    }

    public boolean isConcrete() {
        return !_isVirtual;
    }
}
