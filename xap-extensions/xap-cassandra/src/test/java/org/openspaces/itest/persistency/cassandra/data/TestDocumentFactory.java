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

package org.openspaces.itest.persistency.cassandra.data;

import com.gigaspaces.document.SpaceDocument;

import org.openspaces.test.common.data.TestPojo3;
import org.openspaces.test.common.data.TestPojo4;

public class TestDocumentFactory {

    public static SpaceDocument getTestDocument1(String firstName, String lastName) {
        return new SpaceDocument("TestDocument1")
                .setProperty("firstName", firstName)
                .setProperty("lastName", lastName);
    }

    public static SpaceDocument getTestDocument3(Integer intProperty, Long longProperty, Boolean booleanProperty) {
        return new SpaceDocument("TestDocument3")
                .setProperty("intProperty", intProperty)
                .setProperty("longProperty", longProperty)
                .setProperty("booleanProperty", booleanProperty);
    }

    public static SpaceDocument getTestDocument4(Integer intProperty, TestPojo4 pojo4) {
        return new SpaceDocument("TestDocument4")
                .setProperty("intProperty", intProperty)
                .setProperty("testPojo4", pojo4);
    }

    public static SpaceDocument getTestDocument5(Integer intProperty, SpaceDocument spaceDocument) {
        return new SpaceDocument("TestDocument5")
                .setProperty("intProperty", intProperty)
                .setProperty("spaceDocument", spaceDocument);
    }

    public static SpaceDocument getTestDocument6(TestPojo3 pojo) {
        return new SpaceDocument("TestDocument6")
                .setProperty("testPojo3", pojo);
    }
}
