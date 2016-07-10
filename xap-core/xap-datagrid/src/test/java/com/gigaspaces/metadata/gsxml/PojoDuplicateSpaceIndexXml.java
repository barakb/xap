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

package com.gigaspaces.metadata.gsxml;

import java.io.Serializable;
import java.sql.Date;


@com.gigaspaces.api.InternalApi
public class PojoDuplicateSpaceIndexXml {
    private int id;

    private Info personalInfo;

    private String description;

    public PojoDuplicateSpaceIndexXml() {

    }

    /**
     * @param id
     * @param personalInfo
     */
    public PojoDuplicateSpaceIndexXml(int id, Info personalInfo) {
        super();
        this.id = id;
        this.personalInfo = personalInfo;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Info getPersonalInfo() {
        return personalInfo;
    }

    public void setPersonalInfo(Info personalInfo) {
        this.personalInfo = personalInfo;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String desc) {
        this.description = desc;
    }


    public enum Gender {
        M, F
    }

    public static class Info implements Serializable {
        private String name;

        private Address address;

        private Date birthday;

        private long socialSecurity;
        private int _id;

        public void setSocialSecurity(long socialSecurity) {
            this.socialSecurity = socialSecurity;
        }

        public Info() {
            socialSecurity = 0;
        }

        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }

        public long getSocialSecurity() {
            return socialSecurity;
        }

        public boolean isMarried() {
            return isMarried;
        }

        public void setMarried(boolean isMarried) {
            this.isMarried = isMarried;
        }

        public Gender getGender() {
            return gender;
        }

        public void setGender(Gender gender) {
            this.gender = gender;
        }


        private boolean isMarried;

        private Gender gender;

        /**
         * @param name
         * @param address
         * @param birthday
         * @param socialSecurity
         * @param isMarried
         * @param gender
         */
        public Info(int id, String name, Address address, Date birthday, long socialSecurity, boolean isMarried,
                    Gender gender) {
            super();
            this._id = id;
            this.name = name;
            this.address = address;
            this.birthday = birthday;
            this.socialSecurity = socialSecurity;
            this.isMarried = isMarried;
            this.gender = gender;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getId() {
            return _id;
        }

        public void setId(int id) {
            _id = id;
        }

    }

    public static class Address implements Serializable {
        private int zipCode;
        private String street;

        public Address() {

        }

        /**
         * @param zipCode
         * @param street
         */
        public Address(int zipCode, String street) {
            super();
            this.zipCode = zipCode;
            this.street = street;
        }

        public int getZipCode() {
            return zipCode;
        }

        public String getStreet() {
            return street;
        }

        public void setZipCode(int zipCode) {
            this.zipCode = zipCode;
        }

        public void setStreet(String street) {
            this.street = street;
        }

    }

}
