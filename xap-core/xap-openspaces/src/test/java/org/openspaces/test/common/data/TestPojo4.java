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

package org.openspaces.test.common.data;

import java.util.Date;

public class TestPojo4 {

    private Long longProperty;
    private Date dateProperty;

    public Long getLongProperty() {
        return longProperty;
    }

    public void setLongProperty(Long longProperty) {
        this.longProperty = longProperty;
    }

    public Date getDateProperty() {
        return dateProperty;
    }

    public void setDateProperty(Date dateProperty) {
        this.dateProperty = dateProperty;
    }

    @Override
    public String toString() {
        return "TestPojo4 [longProperty=" + longProperty + ", dateProperty=" + dateProperty + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((dateProperty == null) ? 0 : dateProperty.hashCode());
        result = prime * result
                + ((longProperty == null) ? 0 : longProperty.hashCode());
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
        TestPojo4 other = (TestPojo4) obj;
        if (dateProperty == null) {
            if (other.dateProperty != null)
                return false;
        } else if (!dateProperty.equals(other.dateProperty))
            return false;
        if (longProperty == null) {
            if (other.longProperty != null)
                return false;
        } else if (!longProperty.equals(other.longProperty))
            return false;
        return true;
    }
}
