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

@com.gigaspaces.api.InternalApi
public class ForeignAddress extends Address {
    private static final long serialVersionUID = 1L;

    private String country;

    public String getCountry() {
        return country;
    }

    public ForeignAddress setCountry(String country) {
        this.country = country;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return false;

        ForeignAddress other = (ForeignAddress) obj;
        if (!ObjectUtils.equals(this.country, other.country))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "ForeignAddress [" +
                "street=" + getStreet() +
                ", houseNumber=" + getHouseNumber() +
                ", country=" + getCountry() +
                "]";
    }
}
