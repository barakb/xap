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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents an entry Identifier property.
 *
 * @author Niv Ingberg
 * @since 7.0
 * @deprecated Since 8.0 NOTE: Starting 8.0 this class is not serialized - Externalizable code is
 * maintained for backwards compatibility only.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class IdentifierInfo extends PropertyInfo {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    // If serialization changes, increment GigaspacesVersionID and modify read/writeExternal appropiately.
    private static final byte OldVersionId = 2;

    private boolean _autoGenPk;

    /**
     * Default constructor for {#{@link Externalizable}.
     */
    public IdentifierInfo() {
    }

    public IdentifierInfo(PropertyInfo property, boolean isAutoPk) {
        super(property.getName(), property.getTypeName(), property.getType(), property.getDocumentSupport(), property.getStorageType(), property.getDotnetStorageType());
        this._autoGenPk = isAutoPk;
    }

    public boolean isAutoGenPk() {
        return _autoGenPk;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _autoGenPk = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        out.writeBoolean(_autoGenPk);
    }
}
