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

import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.client.FailedChangedEntryDetails;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class FailedChangedEntryDetailsImpl
        extends ChangeEntryDetailsImpl<Object> implements FailedChangedEntryDetails {
    private static final long serialVersionUID = 1L;
    private Throwable _cause;

    public FailedChangedEntryDetailsImpl() {
    }

    public FailedChangedEntryDetailsImpl(String typeName, Object id, int version, Throwable cause) {
        super(typeName, id, version, null);
        _cause = cause;
    }

    @Override
    public Throwable getCause() {
        return _cause;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _cause);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _cause = IOUtils.readObject(in);
    }

    @Override
    public List<ChangeOperationResult> getChangeOperationsResults() {
        throw new IllegalStateException();
    }

}
