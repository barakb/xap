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

import com.gigaspaces.internal.reflection.fast.ASMFieldFactory;
import com.gigaspaces.internal.reflection.fast.ASMMethodFactory;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Test the ASM packaged/protected field/method access
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class PackagedASMTest {
    /**
     * Test methods access
     */
    @Test
    public void testProtectedFieldAccess() throws Exception {
        fieldAccess("_protectedfield", Object.class, null);
    }

    /**
     * Test methods access
     */
    @Test
    public void testPackagedAccess() throws Exception {
        fieldAccess("_packagedfield", Object.class, null);
    }

    private void fieldAccess(String fieldName, Class clazz, Object nullValue) throws Exception {
        Field field = MyPackagedClass.class.getDeclaredField(fieldName);

        IField iField = ASMFieldFactory.getField(field);

        MyPackagedClass mc = new MyPackagedClass();
        Assert.assertEquals(nullValue, iField.get(mc)); // checks for null value

        iField.set(mc, 11);
        Assert.assertEquals(11, iField.get(mc)); // checks set and get value

        iField.set(mc, nullValue);
        Assert.assertEquals(nullValue, iField.get(mc)); // checks set and get null value
    }

    /**
     * Test methods access
     */
    @Test
    public void testMethodAccess() throws Exception {
        methodsAccess("getPackaged", "setPackaged", Object.class, null);
    }

    /**
     * Test static methods access
     */
    @Test
    public void testStaticFieldAccess() throws Exception {
        methodsAccess("getProtected", "setProtected", Object.class, null);
    }

    private void methodsAccess(String get, String set, Class clazz, Object nullValue) throws Exception {
        Method getMethod = MyPackagedClass.class.getDeclaredMethod(get);
        Method setMethod = MyPackagedClass.class.getDeclaredMethod(set, clazz);

        IMethod iGetMethod = ASMMethodFactory.getMethod(getMethod);
        IMethod iSetmethod = ASMMethodFactory.getMethod(setMethod);

        MyPackagedClass mc = new MyPackagedClass();
        Assert.assertEquals(nullValue, iGetMethod.invoke(mc)); // checks for null value

        iSetmethod.invoke(mc, 11);
        Assert.assertEquals(11, iGetMethod.invoke(mc)); // checks set and get value

        iSetmethod.invoke(mc, new Object[]{nullValue});
        Assert.assertEquals(nullValue, iGetMethod.invoke(mc)); // checks set and get null value

        IGetterMethod getterMethod = ASMMethodFactory.getGetterMethod(getMethod);
        ISetterMethod settermethod = ASMMethodFactory.getSetterMethod(setMethod);

        MyPackagedClass mc2 = new MyPackagedClass();
        Assert.assertEquals(nullValue, getterMethod.get(mc2)); // checks for null value

        settermethod.set(mc2, 11);
        Assert.assertEquals(11, getterMethod.get(mc2)); // checks set and get value

        settermethod.set(mc2, nullValue);
        Assert.assertEquals(nullValue, getterMethod.get(mc2)); // checks set and get null value
    }
}
