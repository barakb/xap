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

import com.gigaspaces.internal.utils.ObjectUtils;

import java.io.Serializable;

@com.gigaspaces.api.InternalApi
public class Address implements Serializable {
    private static final long serialVersionUID = 1L;

    private String street;
    private int houseNumber;

    public String getStreet() {
        return street;
    }

    public Address setStreet(String street) {
        this.street = street;
        return this;
    }

    public int getHouseNumber() {
        return houseNumber;
    }

    public Address setHouseNumber(int houseNumber) {
        this.houseNumber = houseNumber;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Address))
            return false;

        final Address other = (Address) obj;
        if (!ObjectUtils.equals(this.street, other.street))
            return false;
        if (this.houseNumber != other.houseNumber)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "Address [" +
                "street=" + street +
                ", houseNumber=" + houseNumber +
                "]";
    }
}
