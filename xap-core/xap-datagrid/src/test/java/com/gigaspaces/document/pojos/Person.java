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

import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;

@com.gigaspaces.api.InternalApi
public class Person {
    private String name;
    private int age;
    private Address homeAddress;
    private Address workAddress;
    private Address mailAddress;

    public String getName() {
        return name;
    }

    public Person setName(String name) {
        this.name = name;
        return this;
    }

    public int getAge() {
        return age;
    }

    public Person setAge(int age) {
        this.age = age;
        return this;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.CONVERT)
    public Address getHomeAddress() {
        return homeAddress;
    }

    public Person setHomeAddress(Address homeAddress) {
        this.homeAddress = homeAddress;
        return this;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.COPY)
    public Address getWorkAddress() {
        return workAddress;
    }

    public Person setWorkAddress(Address workAddress) {
        this.workAddress = workAddress;
        return this;
    }

    //@SpaceProperty(documentSupport=SpaceDocumentSupport.NOT_SET)
    public Address getMailAddress() {
        return mailAddress;
    }

    public Person setMailAddress(Address mailAddress) {
        this.mailAddress = mailAddress;
        return this;
    }

    @Override
    public String toString() {
        return "Person [" +
                "name=" + name +
                ", age=" + age +
                "homeAddress=" + homeAddress +
                "workAddress=" + workAddress +
                "mailAddress=" + mailAddress +
                "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Person))
            return false;

        Person other = (Person) obj;
        if (!(ObjectUtils.equals(this.name, other.name)))
            return false;
        if (!(ObjectUtils.equals(this.age, other.age)))
            return false;
        if (!(ObjectUtils.equals(this.homeAddress, other.homeAddress)))
            return false;
        if (!(ObjectUtils.equals(this.workAddress, other.workAddress)))
            return false;
        if (!(ObjectUtils.equals(this.mailAddress, other.mailAddress)))
            return false;

        return true;
    }
}
