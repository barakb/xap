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

package com.gigaspaces.internal.query.predicate;

import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@com.gigaspaces.api.InternalApi
public class AssertEx {
    public static void assertExternalizable(Object expected) {
        Object actual = null;

        try {
            // Serialize:
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            out.writeObject(expected);

            out.flush();
            byte[] buffer = outStream.toByteArray();

            // Deserialize
            ByteArrayInputStream inStream = new ByteArrayInputStream(buffer);
            ObjectInputStream in = new ObjectInputStream(inStream);
            actual = in.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to serialize/deserialize 'expected': " + e.toString());
        }

        // Assert:
        Assert.assertEquals(expected, actual);
    }
}
