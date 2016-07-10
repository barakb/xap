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

package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;

import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterReplicationNotificationResponseInfo extends AbstractSpaceResponseInfo {
    private static final long serialVersionUID = 1L;

    public Exception exception;
    public Uuid spaceUID;

    /**
     * Required for Externalizable
     */
    public RegisterReplicationNotificationResponseInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, exception);
        IOUtils.writeObject(out, spaceUID);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.exception = IOUtils.readObject(in);
        this.spaceUID = IOUtils.readObject(in);
    }
}
