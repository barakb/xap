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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceClass.IncludeProperties;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceLeaseExpiration;
import com.gigaspaces.annotation.pojo.SpacePersist;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceVersion;

@SpaceClass(includeProperties = IncludeProperties.EXPLICIT)
@com.gigaspaces.api.InternalApi
public class PojoAttributesInheritence2 extends PojoAttributes {
    private String _id;
    private String _routing;
    private long _leaseExpiration;
    private boolean _persist;
    private int _version;

    @SpaceId
    public String getId2() {
        return _id;
    }

    public void setId2(String id) {
        this._id = id;
    }

    @SpaceRouting
    public String getRouting2() {
        return _routing;
    }

    public void setRouting2(String routing) {
        _routing = routing;
    }

    @SpaceLeaseExpiration
    public long getLeaseExpiration2() {
        return _leaseExpiration;
    }

    public void setLeaseExpiration2(long leaseExpiration) {
        _leaseExpiration = leaseExpiration;
    }

    @SpacePersist
    public boolean getPersist2() {
        return _persist;
    }

    public void setPersist2(boolean persist) {
        _persist = persist;
    }

    @SpaceVersion
    public int getVersion2() {
        return _version;
    }

    public void setVersion2(int version) {
        this._version = version;
    }
}
