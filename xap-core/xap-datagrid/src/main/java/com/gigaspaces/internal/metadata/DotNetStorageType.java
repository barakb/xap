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

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Wrapper class for old .NET StorageType enum.
 *
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class DotNetStorageType {
    private static final byte DEFAULT_CODE = 0;
    private static final byte OBJECT_CODE = 1;
    private static final byte BINARY_CODE = 2;
    private static final byte BINARY_CUSTOM_CODE = 3;
    private static final byte DOCUMENT_CODE = 4;

    public static final byte NULL = toCode(null);

    public static void writeOld(ObjectOutput out, byte code)
            throws IOException {
        StorageType storageType = DotNetStorageType.fromCode(code);
        IOUtils.writeObject(out, storageType);
    }

    public static byte readOld(ObjectInput in)
            throws IOException, ClassNotFoundException {
        StorageType storageType = IOUtils.readObject(in);
        return toCode(storageType);
    }

    private static byte toCode(StorageType storageType) {
        if (storageType == null)
            return OBJECT_CODE;

        switch (storageType) {
            case DEFAULT:
                return DEFAULT_CODE;
            case OBJECT:
                return OBJECT_CODE;
            case BINARY:
                return BINARY_CODE;
            case BINARY_CUSTOM:
                return BINARY_CUSTOM_CODE;
            case DOCUMENT:
                return DOCUMENT_CODE;
            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }

    private static StorageType fromCode(byte code) {
        switch (code) {
            case DEFAULT_CODE:
                return StorageType.DEFAULT;
            case OBJECT_CODE:
                return StorageType.OBJECT;
            case BINARY_CODE:
                return StorageType.BINARY;
            case BINARY_CUSTOM_CODE:
                return StorageType.BINARY_CUSTOM;
            case DOCUMENT_CODE:
                return StorageType.DOCUMENT;
            default:
                throw new IllegalArgumentException("Unsupported storage type code: " + code);
        }
    }
}
