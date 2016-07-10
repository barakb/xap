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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Test the ASM field access
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ASMFieldTest {
    /**
     * Test public field access
     */
    @Test
    public void testFieldAccess() throws Exception {
        fieldsAccess("_field", Object.class, null);
    }

    /**
     * Test public field access
     */
    @Test
    public void testIntFieldAccess() throws Exception {
        fieldsAccess("_intField", Integer.TYPE, 0);
    }

    /**
     * Test Packaged field access
     */
    @Test
    public void testPackagedFieldAccess() throws Exception {
        fieldsAccess("_packagedField", Object.class, null);
    }

    /**
     * Test Packaged field access
     */
    @Test
    public void testPackagedIntFieldAccess() throws Exception {
        fieldsAccess("_packagedIntField", Integer.TYPE, 0);
    }

    /**
     * Test Protected field access
     */
    @Test
    public void testProtectedFieldAccess() throws Exception {
        fieldsAccess("_protectedField", Object.class, null);
    }

    /**
     * Test Protected field access
     */
    @Test
    public void testProtectedIntFieldAccess() throws Exception {
        fieldsAccess("_protectedIntField", Integer.TYPE, 0);
    }

    /**
     * Test Protected field access
     */
    @Test
    public void testStaticFieldAccess() throws Exception {
        fieldsAccess("_staticField", Object.class, null);
    }

    /**
     * Test Protected field access
     */
    @Test
    public void testStaticIntFieldAccess() throws Exception {
        fieldsAccess("_staticIntField", Integer.TYPE, 0);
    }

    /**
     * Test private field access
     */
    @Test
    public void testPrivateFieldAccess() throws Exception {
        try {
            fieldsAccess("_privateField", Object.class, null);
            Assert.fail();
        } catch (IllegalAccessError e) {

        }
    }

    /**
     * Test private field access
     */
    @Test
    public void testPrivateIntFieldAccess() throws Exception {
        try {
            fieldsAccess("_privateIntField", Integer.TYPE, 0);
            Assert.fail();
        } catch (IllegalAccessError e) {

        }
    }

    private void fieldsAccess(String fieldName, Class clazz, Object nullValue) throws Exception {
        Field field = MyClass.class.getDeclaredField(fieldName);
        IField iField = ASMFieldFactory.getField(field);

        Assert.assertEquals(clazz, iField.getType());
        MyClass mc = new MyClass();
        Assert.assertEquals(nullValue, iField.get(mc)); // checks for null value

        iField.set(mc, 11);
        Assert.assertEquals(11, iField.get(mc)); // checks set and get value

        iField.set(mc, nullValue);
        Assert.assertEquals(nullValue, iField.get(mc)); // checks set and get null value
    }

    public static class MyClass {
        public Object _field;
        public int _intField;

        Object _packagedField;
        int _packagedIntField;

        protected Object _protectedField;
        protected int _protectedIntField;

        private Object _privateField;
        private int _privateIntField;

        public static Object _staticField;
        public static int _staticIntField;

    }

    /**
     * Test Inner private type field access
     */
    @Test
    public void testInnerClassWithInnerType() throws Exception {
        Field field = InnerClassWithInnerType.class.getDeclaredField("node");
        IField iField = ASMFieldFactory.getField(field);
        InnerClassWithInnerType mc = new InnerClassWithInnerType();
        Assert.assertEquals(null, iField.get(mc)); // checks for null value
        InnerClassWithInnerType.InnerPrivateType testNode = new InnerClassWithInnerType.InnerPrivateType();
        iField.set(mc, testNode);
        Assert.assertSame(testNode, iField.get(mc)); // checks set and get value
    }

    /**
     * Class with inner private type
     */
    public static class InnerClassWithInnerType {
        public InnerPrivateType node;

        private static class InnerPrivateType {
            InnerPrivateType() {
            }
        }

        public InnerClassWithInnerType() {
        }
    }

    /**
     * Test Anonymous type field access
     */
    @Test
    public void testAnonymousClassWithInnerType() throws Exception {

        Object anonymous = new Object() {
            public int _field;
        };

        Field field = anonymous.getClass().getDeclaredField("_field");
        IField iField = ASMFieldFactory.getField(field);
        Assert.assertEquals(0, iField.get(anonymous)); // checks for null value
        iField.set(anonymous, 1);
        Assert.assertSame(1, iField.get(anonymous)); // checks set and get value
    }

    /**
     * Test Anonymous type field access
     */
    @Test
    public void testPrivateInnerType() throws Exception {

        MyPrivateClass inner = new MyPrivateClass();

        Field field = inner.getClass().getDeclaredField("_field");
        IField iField = ASMFieldFactory.getField(field);
        Assert.assertEquals(null, iField.get(inner)); // checks for null value
        iField.set(inner, 1);
        Assert.assertSame(1, iField.get(inner)); // checks set and get value
    }

    private static class MyPrivateClass {
        public Object _field;
    }
}
