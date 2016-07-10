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

package com.gigaspaces.lrmi.classloading;

/**
 * A unique identifier for a specific remote lrmi class loader, holds a tuple of the lrmi runtime id
 * and the local class loader id inside that lrmi runtime class provider
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class LRMIRemoteClassLoaderIdentifier {
    //Use of Long due to hascode method.
    private final long _remoteLrmiRuntimeId;
    private final long _remoteClassLoaderId;

    public LRMIRemoteClassLoaderIdentifier(long remoteLrmiRuntimeId, long remoteClassLoaderId) {
        _remoteLrmiRuntimeId = remoteLrmiRuntimeId;
        _remoteClassLoaderId = remoteClassLoaderId;
    }

    @Override
    public int hashCode() {
        return getLongHashCode(_remoteClassLoaderId) ^ getLongHashCode(_remoteLrmiRuntimeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LRMIRemoteClassLoaderIdentifier))
            return false;

        LRMIRemoteClassLoaderIdentifier other = (LRMIRemoteClassLoaderIdentifier) obj;

        return _remoteLrmiRuntimeId == other._remoteLrmiRuntimeId && _remoteClassLoaderId == other._remoteClassLoaderId;
    }

    public int getLongHashCode(long value) {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "remote lrmi runtime id = " + _remoteLrmiRuntimeId + ", remote class loader id = " + _remoteClassLoaderId;
    }

    public long getRemoteClassLoaderId() {
        return _remoteClassLoaderId;
    }

    public long getRemoteLrmiRuntimeId() {
        return _remoteLrmiRuntimeId;
    }

}
