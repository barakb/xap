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

package com.gigaspaces.internal.lease;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.ObjectTypes;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryLease<T> extends SpaceLease implements LeaseContext<T> {
    private int _version;
    private T _previousObject;

    public SpaceEntryLease(IDirectSpaceProxy spaceProxy, String typeName,
                           String uid, int version, Object routingValue, long expiration, T prevEntry) {
        super(spaceProxy, typeName, uid, routingValue, expiration);
        this._version = version;
        this._previousObject = prevEntry;
    }

    @Override
    protected int getLeaseObjectType() {
        return ObjectTypes.ENTRY;
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public T getObject() {
        return _previousObject;
    }

    public void setObject(T object) {
        _previousObject = object;
    }
}
