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

import com.gigaspaces.internal.reflection.fast.ASMMethodFactory;
import com.gigaspaces.internal.reflection.standard.StandardMethod;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

/**
 * Test the ASM methods access
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ASMMethodTest {
    /**
     * Test methods access
     */
    @Test
    public void testMethodAccess() throws Exception {
        methodsAccess("getField", "setField", Object.class, null);
    }

    /**
     * Test static methods access
     */
    @Test
    public void testStaticFieldAccess() throws Exception {
        methodsAccess("getStaticField", "setStaticField", Object.class, null);
    }

    /**
     * Test methods access
     */
    @Test
    public void testIntMethodAccess() throws Exception {
        methodsAccess("getIntField", "setIntField", Integer.TYPE, 0);
    }

    /**
     * Test static methods access
     */
    @Test
    public void testStaticIntFieldAccess() throws Exception {
        methodsAccess("getStaticIntField", "setStaticIntField", Integer.TYPE, 0);
    }

    @Test
    public void testNonStaticClass() throws Exception {
        MyNonStaticClass test = new MyNonStaticClass();
        IMethod method = ASMMethodFactory.getMethod(test.getClass().getMethod("test"));
        Integer retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, retVal.intValue());
    }

    @Test
    public void testNonStaticPrivateClass() throws Exception {
        MyNonStaticPrivateClass test = new MyNonStaticPrivateClass();
        IMethod method = ASMMethodFactory.getMethod(test.getClass().getMethod("test"));
        Integer retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);

        method = ReflectionUtil.createMethod(test.getClass().getMethod("test"));
        retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);
    }

    /**
     * Tests that anonymous class fails with ASM, but works with ReflectionUtil since it will
     * fallback to standard.
     */
    @Test
    public void testAnonymousClass() throws Exception {
        Object test = new Object() {

            public int test() {
                return 1;
            }
        };
        Method method1 = test.getClass().getMethod("test");
        IMethod method = ASMMethodFactory.getMethod(method1);
        Integer retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);

        method1 = test.getClass().getMethod("test");
        method = ReflectionUtil.createMethod(method1);
        retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);
    }

    @Test
    public void testAnonymousClassWithInterface() throws Exception {
        Object test = new MyInterface() {

            public int test() {
                return 1;
            }
        };
        Method method1 = MyInterface.class.getMethod("test");
        IMethod method = ASMMethodFactory.getMethod(method1);
        Integer retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);
    }

    @Test
    public void testClassWithInterface() throws Exception {
        MyNonStaticClassWithInterface test = new MyNonStaticClassWithInterface();
        Method method1 = MyInterface.class.getMethod("test");
        IMethod method = ASMMethodFactory.getMethod(method1);
        Integer retVal = (Integer) method.invoke(test);
        Assert.assertEquals(1, (int) retVal);
    }

    /**
     * Checks that repeat call returns the same class
     */
    @Test
    public void testRepeatGet() throws Exception {
        MyNonStaticClassWithInterface test = new MyNonStaticClassWithInterface();
        Method method1 = MyInterface.class.getMethod("test");

        IMethod method = ASMMethodFactory.getMethod(method1);
        Assert.assertNotSame(method.getClass(), StandardMethod.class);
        Assert.assertEquals(method.getClass(), ASMMethodFactory.getMethod(method1).getClass());
        Assert.assertEquals(method.getClass(), ASMMethodFactory.getMethod(method1).getClass());
    }

    /**
     * Checks that id the user throws an exception
     */
    @Test
    public void testMethodException() throws Exception {
        IMethod method = ASMMethodFactory.getMethod(ExceptionClass.class.getMethod("test"));
        try {
            method.invoke(new ExceptionClass());
            Assert.assertTrue("Should throw InvocationTargetException.", false);
        } catch (InvocationTargetException e) {
            Assert.assertTrue("Wrong inetrnal exception", e.getCause() instanceof ExecutionException);
        }
    }

    private void methodsAccess(String get, String set, Class clazz, Object nullValue) throws Exception {
        Method getMethod = MyClass.class.getDeclaredMethod(get);
        Method setMethod = MyClass.class.getDeclaredMethod(set, clazz);

        IMethod iGetMethod = ASMMethodFactory.getMethod(getMethod);
        IMethod iSetmethod = ASMMethodFactory.getMethod(setMethod);

        MyClass mc = new MyClass();
        Assert.assertEquals(nullValue, iGetMethod.invoke(mc)); // checks for null value

        iSetmethod.invoke(mc, 11);
        Assert.assertEquals(11, iGetMethod.invoke(mc)); // checks set and get value

        iSetmethod.invoke(mc, new Object[]{nullValue});
        Assert.assertEquals(nullValue, iGetMethod.invoke(mc)); // checks set and get null value

        IGetterMethod getterMethod = ASMMethodFactory.getGetterMethod(getMethod);
        ISetterMethod settermethod = ASMMethodFactory.getSetterMethod(setMethod);

        MyClass mc2 = new MyClass();
        Assert.assertEquals(nullValue, getterMethod.get(mc2)); // checks for null value

        settermethod.set(mc2, 11);
        Assert.assertEquals(11, getterMethod.get(mc2)); // checks set and get value

        settermethod.set(mc2, nullValue);
        Assert.assertEquals(nullValue, getterMethod.get(mc2)); // checks set and get null value
    }

    public interface MyInterface {

        int test();
    }

    public class ExceptionClass {
        public int test() throws ExecutionException {
            throw new ExecutionException("Error", new Exception());
        }
    }

    public class MyNonStaticClassWithInterface implements MyInterface {

        public int test() {
            return 1;
        }
    }

    public class MyNonStaticClass {

        public int test() {
            return 1;
        }
    }

    private class MyNonStaticPrivateClass {

        public int test() {
            return 1;
        }
    }

    public static class MyClass {
        private Object _field;
        private static Object _staticField;

        private int _intfield;
        private static int _staticIntField;

        public Object getField() {
            return _field;
        }

        public void setField(Object _field) {
            this._field = _field;
        }

        public static Object getStaticField() {
            return _staticField;
        }

        public static void setStaticField(Object _field) {
            _staticField = _field;
        }

        public int getIntField() {
            return _intfield;
        }

        public void setIntField(int _field) {
            this._intfield = _field;
        }

        public static int getStaticIntField() {
            return _staticIntField;
        }

        public static void setStaticIntField(int _field) {
            _staticIntField = _field;
        }
    }
}
