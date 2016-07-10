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

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;

import java.net.URI;
import java.util.Locale;

@SpaceClass()
@com.gigaspaces.api.InternalApi
public class PojoWithComplexJavaProperties {

    private Integer id;
    private Class entityClass;
    private URI entityURI;
    private Locale entityLocale;

    public PojoWithComplexJavaProperties() {
        super();
    }

    public URI getEntityURI() {
        return entityURI;
    }

    public void setEntityURI(URI entityURI) {
        this.entityURI = entityURI;
    }

    public Locale getEntityLocale() {
        return entityLocale;
    }

    public void setEntityLocale(Locale entityLocale) {
        this.entityLocale = entityLocale;
    }

    @SpaceIndex
    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((entityClass == null) ? 0 : entityClass.hashCode());
        result = prime * result
                + ((entityLocale == null) ? 0 : entityLocale.hashCode());
        result = prime * result
                + ((entityURI == null) ? 0 : entityURI.hashCode());
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
        PojoWithComplexJavaProperties other = (PojoWithComplexJavaProperties) obj;
        if (entityClass == null) {
            if (other.entityClass != null)
                return false;
        } else if (entityClass != entityClass)
            return false;
        if (entityLocale == null) {
            if (other.entityLocale != null)
                return false;
        } else if (!entityLocale.equals(other.entityLocale))
            return false;
        if (entityURI == null) {
            if (other.entityURI != null)
                return false;
        } else if (!entityURI.equals(other.entityURI))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}
