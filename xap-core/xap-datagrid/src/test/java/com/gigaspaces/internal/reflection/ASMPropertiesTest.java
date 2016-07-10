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

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.reflection.fast.ASMPropertiesFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test the ASM properties access
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ASMPropertiesTest {
    @Test
    public void testObjectClass() throws Exception {
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(Object.class);
        IProperties<Object> properties = ASMPropertiesFactory.getProperties(typeInfo);
        Assert.assertNotNull(properties);
    }

    @Test
    public void testSetValues() throws Exception {
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(ASMPropertiesTestObject.class);
        IProperties<Object> properties = ASMPropertiesFactory.getProperties(typeInfo);

        ASMPropertiesTestObject instance = new ASMPropertiesTestObject();
        Object[] input = new Object[]{1, "aa", "bb", 2, 3};
        properties.setValues(instance, input);
        Object[] output = properties.getValues(instance);
        Assert.assertTrue(Arrays.equals(input, output));
    }

    @Test
    public void testSetValuesWithNullValue() throws Exception {
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(ASMPropertiesTestObject.class);
        IProperties<Object> properties = ASMPropertiesFactory.getProperties(typeInfo);

        ASMPropertiesTestObject instance = new ASMPropertiesTestObject();
        Object[] input = new Object[]{null, "bb", "aa", null, null};
        properties.setValues(instance, input);
        Object[] output = properties.getValues(instance);

        Object[] expectedOutput = new Object[]{-1, "bb", "aa", 0, ASMPropertiesTestObject.DEFAULT_Y};
        Assert.assertTrue(Arrays.equals(expectedOutput, output));
    }
}
