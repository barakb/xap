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

package com.gigaspaces.internal.query.continous;

import com.gigaspaces.events.NotifyActionType;

import net.jini.core.lease.Lease;

/**
 * @author Niv Ingberg
 * @since 8.0.3
 */
@com.gigaspaces.api.InternalApi
public class ContinousQueryConfig {
    private long _leaseDuration;
    private NotifyActionType _notifyActionType;
    private boolean _returnOnlyUid;
    private int _readModifiers;

    public ContinousQueryConfig() {
        this._leaseDuration = Lease.FOREVER;
        this._notifyActionType = NotifyActionType.NOTIFY_ALL;
        this._returnOnlyUid = false;
    }

    public long getLeaseDuration() {
        return _leaseDuration;
    }

    public ContinousQueryConfig setLeaseDuration(long leaseDuration) {
        this._leaseDuration = leaseDuration;
        return this;
    }

    public NotifyActionType getNotifyActionType() {
        return _notifyActionType;
    }

    public ContinousQueryConfig setNotifyActionType(NotifyActionType notifyActionType) {
        this._notifyActionType = notifyActionType;
        return this;
    }

    public boolean isReturnOnlyUid() {
        return _returnOnlyUid;
    }

    public ContinousQueryConfig setReturnOnlyUid(boolean returnOnlyUid) {
        this._returnOnlyUid = returnOnlyUid;
        return this;
    }

    public int getReadModifiers() {
        return _readModifiers;
    }

    public ContinousQueryConfig setReadModifiers(int readModifiers) {
        _readModifiers = readModifiers;
        return this;
    }
}
