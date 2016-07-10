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

package com.gigaspaces.persistency.qa.model;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;

@SpaceClass
public class SmallTypeHierarcyMongoDBSpaceDataClassA implements
        Comparable<SmallTypeHierarcyMongoDBSpaceDataClassA> {

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aProp == null) ? 0 : aProp.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        SmallTypeHierarcyMongoDBSpaceDataClassA other = (SmallTypeHierarcyMongoDBSpaceDataClassA) obj;
        if (aProp == null) {
            if (other.aProp != null)
                return false;
        } else if (!aProp.equals(other.aProp))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    private String id;
    private String aProp;

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @SpaceProperty
    public String getAProp() {
        return aProp;
    }

    public void setAProp(String aProp) {
        this.aProp = aProp;
    }

    public int compareTo(SmallTypeHierarcyMongoDBSpaceDataClassA o) {
        if (id.equals(o.id))
            return 0;
        return id.hashCode() - o.id.hashCode();
    }

}
