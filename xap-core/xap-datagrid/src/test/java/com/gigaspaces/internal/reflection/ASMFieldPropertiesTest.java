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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.reflection.fast.ASMFieldPropertiesFactory;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Test the ASM properties access
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ASMFieldPropertiesTest {
    final private Field[] fields = new Field[4];

    public ASMFieldPropertiesTest() throws Exception {
        fields[0] = ASMFieldPropertiesTestObject.class.getDeclaredField("x");
        fields[1] = ASMFieldPropertiesTestObject.class.getDeclaredField("y");
        fields[2] = ASMFieldPropertiesTestObject.class.getDeclaredField("o");
        fields[3] = ASMFieldPropertiesTestObject.class.getDeclaredField("s");
    }

    @Test
    public void testClass() throws Exception {
        IProperties<Object> properties = ASMFieldPropertiesFactory.getProperties(ASMFieldPropertiesTestObject.class, fields);
        ASMFieldPropertiesTestObject test = new ASMFieldPropertiesTestObject();
        Object[] input = new Object[]{1, 2L, "aa", "bb"};
        properties.setValues(test, input);
        Object[] output = properties.getValues(test);

        Assert.assertTrue(Arrays.equals(input, output));
    }
}
