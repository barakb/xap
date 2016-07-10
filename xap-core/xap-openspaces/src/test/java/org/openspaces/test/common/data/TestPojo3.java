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

import java.io.Serializable;

public class TestPojo3 implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private Integer age;
    private TestPojo4 pojo4_1;
    private TestPojo4 pojo4_2;

    public TestPojo3() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public TestPojo4 getPojo4_1() {
        return pojo4_1;
    }

    public void setPojo4_1(TestPojo4 pojo4) {
        this.pojo4_1 = pojo4;
    }

    public TestPojo4 getPojo4_2() {
        return pojo4_2;
    }

    public void setPojo4_2(TestPojo4 pojo4) {
        pojo4_2 = pojo4;
    }

    @Override
    public String toString() {
        return "TestPojo3 [name=" + name + ", age=" + age + ", pojo4_1=" + pojo4_1 + ", pojo4_2=" + pojo4_2 + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((age == null) ? 0 : age.hashCode());
        result = prime
                * result
                + ((pojo4_1 == null) ? 0
                : pojo4_1.hashCode());
        result = prime
                * result
                + ((pojo4_2 == null) ? 0
                : pojo4_2.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        TestPojo3 other = (TestPojo3) obj;
        if (age == null) {
            if (other.age != null)
                return false;
        } else if (!age.equals(other.age))
            return false;
        if (pojo4_1 == null) {
            if (other.pojo4_1 != null)
                return false;
        } else if (!pojo4_1.equals(other.pojo4_1))
            return false;
        if (pojo4_2 == null) {
            if (other.pojo4_2 != null)
                return false;
        } else if (!pojo4_2.equals(other.pojo4_2))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }
}