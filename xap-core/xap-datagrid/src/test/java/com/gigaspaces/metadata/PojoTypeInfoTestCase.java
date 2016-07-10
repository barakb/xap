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

package com.gigaspaces.metadata;

import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfoRepository;
import com.gigaspaces.metadata.annotated.PojoCommonProperties;
import com.gigaspaces.metadata.pojos.PojoAccessors;
import com.gigaspaces.metadata.pojos.PojoAccessors2;
import com.gigaspaces.metadata.pojos.PojoCommonNames;
import com.gigaspaces.metadata.pojos.PojoCommonSetters;
import com.gigaspaces.metadata.pojos.PojoIllegalProperties;
import com.gigaspaces.metadata.pojos.PojoNoProperties;
import com.gigaspaces.metadata.pojos.PojoOneProperty;

import junit.framework.Assert;
import junit.framework.TestCase;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

@com.gigaspaces.api.InternalApi
public class PojoTypeInfoTestCase extends TestCase {
    public void testNull() {
        // Arrange:
        final Class<?> type = null;

        // Act/Assert:
        try {
            PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);
            Assert.fail("Expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testObject() throws Exception {
        testPojoTypeInfo(Object.class);
        testPojoTypeInfo(PojoNoProperties.class);
        testPojoTypeInfo(PojoOneProperty.class);
        testPojoTypeInfo(PojoCommonProperties.class);
        testPojoTypeInfo(PojoCommonNames.class);

        // Test PojoIllegalProperties
        PojoTypeInfo actualTypeInfo = PojoTypeInfoRepository.getPojoTypeInfo(PojoIllegalProperties.class);
        Assert.assertEquals("numOfProperties", 2, actualTypeInfo.getNumOfProperties());
        Assert.assertEquals("type", Class.class, actualTypeInfo.getProperty("class").getType());
        Assert.assertEquals("type", String.class, actualTypeInfo.getProperty("ID").getType());
    }

    /**
     * Tested manually because java's Introspector requires setters to return void...
     */
    public void testPojoCommonSetters() {
        // Arrange:
        final Class<?> type = PojoCommonSetters.class;

        // Act:
        PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        // Assert:
        assertPojoTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 5, typeInfo.getNumOfProperties());
        assertPojoPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPojoPropertyInfo(typeInfo, "propertySimple", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "propertyOriginal", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "propertyFluent", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "propertyOverload", String.class, true, true);
    }

    /**
     * Tested manually because java's Introspector only gets public methods.
     */
    public void testPojoAccessors() {
        // Arrange:
        final Class<?> type = PojoAccessors.class;

        // Act:
        PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        // Assert:
        assertPojoTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 5, typeInfo.getNumOfProperties());
        assertPojoPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPojoPropertyInfo(typeInfo, "publicName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "protectedName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "privateName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "defaultName", String.class, true, true);
    }

    /**
     * Tested manually because java's Introspector only gets public methods.
     */
    public void testPojoAccessors2() {
        // Arrange:
        final Class<?> type = PojoAccessors2.class;

        // Act:
        PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        // Assert:
        assertPojoTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPojoPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPojoPropertyInfo(typeInfo, "publicName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "protectedName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "privateName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "defaultName", String.class, true, true);
        assertPojoPropertyInfo(typeInfo, "extraName", String.class, true, true);

        Assert.assertEquals(type, typeInfo.getProperty("publicName").getGetterMethod().getDeclaringClass());
        Assert.assertEquals(type, typeInfo.getProperty("publicName").getSetterMethod().getDeclaringClass());
        Assert.assertEquals(type, typeInfo.getProperty("protectedName").getGetterMethod().getDeclaringClass());
        Assert.assertEquals(type, typeInfo.getProperty("protectedName").getSetterMethod().getDeclaringClass());
        Assert.assertEquals(type, typeInfo.getProperty("privateName").getGetterMethod().getDeclaringClass());
        Assert.assertEquals(type, typeInfo.getProperty("privateName").getSetterMethod().getDeclaringClass());
    }

    private static void assertPojoTypeInfo(PojoTypeInfo typeInfo, Class<?> type) {
        Assert.assertEquals("getType", type, typeInfo.getType());
        Assert.assertEquals("getName", type.getName(), typeInfo.getName());

        Class<?> superType = type.getSuperclass();
        if (superType == null)
            Assert.assertEquals(null, typeInfo.getSuperTypeInfo());
        else
            assertPojoTypeInfo(typeInfo.getSuperTypeInfo(), superType);
    }

    private static void assertPojoPropertyInfo(PojoTypeInfo typeInfo, String name, Class<?> type,
                                               boolean hasGetter, boolean hasSetter) {
        PojoPropertyInfo property = typeInfo.getProperty(name);
        Assert.assertNotNull("property [" + name + "]", property);
        Assert.assertEquals("name", name, property.getName());
        Assert.assertEquals("type", type, property.getType());

        Method getter = property.getGetterMethod();
        if (hasGetter) {
            Assert.assertNotNull("getter", getter);
            Assert.assertEquals("getter parameters", 0, getter.getParameterTypes().length);
            Assert.assertEquals("getter type", property.getType(), getter.getReturnType());
        } else
            Assert.assertNull("getter", getter);

        Method setter = property.getSetterMethod();
        if (hasSetter) {
            Assert.assertNotNull("setter", setter);
            Assert.assertEquals("setter parameters", 1, setter.getParameterTypes().length);
            Assert.assertEquals("setter type", property.getType(), setter.getParameterTypes()[0]);
        } else
            Assert.assertNull("setter", setter);
    }

    private static void testPojoTypeInfo(Class<?> type)
            throws IntrospectionException {
        // Act:
        PojoTypeInfo actualTypeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        // Assert:
        final BeanInfo expectedTypeInfo = Introspector.getBeanInfo(type);

        PropertyDescriptor[] expectedProperties = expectedTypeInfo.getPropertyDescriptors();

        int length = expectedProperties.length;
        Assert.assertEquals("numOfProperties", length, actualTypeInfo.getNumOfProperties());

        for (int i = 0; i < length; i++) {
            PropertyDescriptor expectedProperty = expectedProperties[i];
            PojoPropertyInfo actualProperty = actualTypeInfo.getProperty(expectedProperty.getName());
            Assert.assertNotNull(actualProperty);
            Assert.assertEquals("name", expectedProperty.getName(), actualProperty.getName());
            Assert.assertEquals("type", expectedProperty.getPropertyType(), actualProperty.getType());
            Assert.assertEquals("getter", expectedProperty.getReadMethod(), actualProperty.getGetterMethod());
            Assert.assertEquals("setter", expectedProperty.getWriteMethod(), actualProperty.getSetterMethod());
        }
    }
}
