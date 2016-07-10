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

package com.gigaspaces.document.pojos;

import com.gigaspaces.annotation.pojo.SpaceClassConstructor;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceLeaseExpiration;
import com.gigaspaces.annotation.pojo.SpacePersist;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceVersion;

@com.gigaspaces.api.InternalApi
public class ConstructorBasedPojo {

    final Integer id;
    final Integer routing;
    final String name;
    private final long lease;
    private int version;
    private final boolean persist;

    @SpaceClassConstructor
    public ConstructorBasedPojo(Integer id, Integer routing, String name, long lease, int version, boolean persist) {
        this.id = id;
        this.name = name;
        this.routing = routing;
        this.lease = lease;
        this.version = version;
        this.persist = persist;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @SpaceRouting
    public Integer getRouting() {
        return routing;
    }

    @SpaceLeaseExpiration
    public long getLease() {
        return lease;
    }

    @SpaceVersion
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @SpacePersist
    public boolean isPersist() {
        return persist;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + (int) (lease ^ (lease >>> 32));
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (persist ? 1231 : 1237);
        result = prime * result + ((routing == null) ? 0 : routing.hashCode());
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConstructorBasedPojo other = (ConstructorBasedPojo) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (lease != other.lease)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (persist != other.persist)
            return false;
        if (routing == null) {
            if (other.routing != null)
                return false;
        } else if (!routing.equals(other.routing))
            return false;
        if (version != other.version)
            return false;
        return true;
    }
}
