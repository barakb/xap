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

import com.gigaspaces.annotation.pojo.SpaceClass.IncludeProperties;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class IncludePropertiesHelper {
    private static final byte CODE_NULL = -1;
    private static final byte CODE_IMPLICIT = 0;
    private static final byte CODE_EXPLICIT = 1;

    public static void writeExternal(ObjectOutput out, IncludeProperties value)
            throws IOException {
        byte code = CODE_NULL;

        if (value != null) {
            switch (value) {
                case IMPLICIT:
                    code = CODE_IMPLICIT;
                    break;
                case EXPLICIT:
                    code = CODE_EXPLICIT;
                    break;
                default:
                    code = CODE_NULL;
                    break;
            }
        }
        out.writeByte(code);
    }

    public static IncludeProperties readExternal(ObjectInput in)
            throws IOException {
        byte code = in.readByte();
        switch (code) {
            case CODE_NULL:
                return null;
            case CODE_IMPLICIT:
                return IncludeProperties.IMPLICIT;
            case CODE_EXPLICIT:
                return IncludeProperties.EXPLICIT;
            default:
                return null;
        }
    }
}
