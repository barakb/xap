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

import com.gigaspaces.document.SpaceDocument;

import java.io.Serializable;

public class TestPojo1 implements Serializable {

    private static final long serialVersionUID = 1L;

    private String str;
    private SpaceDocument spaceDocument;

    public TestPojo1() {
    }

    public TestPojo1(String str) {
        this.setStr(str);
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public SpaceDocument getSpaceDocument() {
        return spaceDocument;
    }

    public void setSpaceDocument(SpaceDocument spaceDocument) {
        this.spaceDocument = spaceDocument;
    }

    @Override
    public String toString() {
        return "TestPojo1 [spaceDocument=" + spaceDocument + ", str=" + str + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((spaceDocument == null) ? 0 : spaceDocument.hashCode());
        result = prime * result + ((str == null) ? 0 : str.hashCode());
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
        TestPojo1 other = (TestPojo1) obj;
        if (spaceDocument == null) {
            if (other.spaceDocument != null)
                return false;
        } else if (!spaceDocument.equals(other.spaceDocument))
            return false;
        if (str == null) {
            if (other.str != null)
                return false;
        } else if (!str.equals(other.str))
            return false;
        return true;
    }
}