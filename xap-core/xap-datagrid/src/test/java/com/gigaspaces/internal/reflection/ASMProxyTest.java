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

import com.gigaspaces.internal.reflection.fast.proxy.AbstractProxy;
import com.gigaspaces.internal.reflection.fast.proxy.AbstractProxy.ProxyReplace;
import com.gigaspaces.internal.reflection.fast.proxy.ProxyFactory;
import com.gigaspaces.internal.reflection.standard.StandardMethod;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Test the {@link ProxyFactory}
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ASMProxyTest {
    @Test
    public void testRepeatProxyCreate() {
        Handler handler1 = new Handler();
        Object proxyInstance1 = ProxyFactory.newProxyInstance(ASMProxyTest.class.getClassLoader(),
                new Class[]{InterfaceA.class, InterfaceB.class}, handler1, true);

        Handler handler2 = new Handler();
        Object proxyInstance2 = ProxyFactory.newProxyInstance(ASMProxyTest.class.getClassLoader(),
                new Class[]{InterfaceA.class, InterfaceB.class}, handler2, true);

        Assert.assertSame(proxyInstance1.getClass(), proxyInstance2.getClass());

        ArrayList<IMethod> invokeList1 = handler1.getInvokeList();
        ((InterfaceA) proxyInstance1).foo1();
        Assert.assertEquals(1, invokeList1.size());

        ArrayList<IMethod> invokeList2 = handler2.getInvokeList();
        ((InterfaceA) proxyInstance2).foo1();
        Assert.assertEquals(1, invokeList2.size());

        Assert.assertEquals(invokeList1, invokeList2);
    }

    @Test
    public void testProxyCreate() {
        Handler handler = new Handler();
        Object newProxyInstance = ProxyFactory.newProxyInstance(ASMProxyTest.class.getClassLoader(),
                new Class[]{InterfaceA.class, InterfaceB.class}, handler, true);

        checkProxy(newProxyInstance, handler);
    }

    @Test
    public void testNoClassLoaderCreate() {
        Object newProxyInstance = ProxyFactory.newProxyInstance(null,
                new Class[]{InterfaceA.class, InterfaceB.class}, new Handler(), true);
        Assert.assertNotNull(newProxyInstance);

    }

    @Test
    public void testProxySerialization() throws Exception {
        Handler handler = new Handler();
        Object newProxyInstance = ProxyFactory.newProxyInstance(ASMProxyTest.class.getClassLoader(),
                new Class[]{InterfaceA.class, InterfaceB.class}, handler, true);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(newProxyInstance);

        Object readObject = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())).readObject();

        checkProxy(readObject, (Handler) ReflectionUtil.getInvocationHandler(readObject));
    }

    @Test
    public void testProxyReplace() throws Exception {
        Handler handler = new Handler();
        AbstractProxy newProxyInstance = (AbstractProxy) ProxyFactory.newProxyInstance(ASMProxyTest.class.getClassLoader(),
                new Class[]{InterfaceA.class, InterfaceB.class}, handler, true);
        ProxyReplace proxyReplace = (ProxyReplace) newProxyInstance.writeReplace();
        Object readObject = proxyReplace.readResolve();
        Assert.assertNotSame(newProxyInstance, readObject);
        checkProxy(readObject, (Handler) ReflectionUtil.getInvocationHandler(readObject));
    }

    private void checkProxy(Object newProxyInstance, Handler handler) {
        // check the right interfaces
        Assert.assertNotNull(newProxyInstance);
        Assert.assertTrue(newProxyInstance instanceof InterfaceA);
        Assert.assertTrue(newProxyInstance instanceof InterfaceB);
        Assert.assertTrue(newProxyInstance instanceof AbstractProxy);

        // check invocation
        InterfaceA proxyA = (InterfaceA) newProxyInstance;
        proxyA.foo1();
        Assert.assertEquals(4343, proxyA.foo2(new Object(), 4343.45d));
        Assert.assertEquals("233434453425345 44", proxyA.foo3(233434453425345L, (byte) 44));

        InterfaceB proxyB = (InterfaceB) newProxyInstance;
        Assert.assertEquals(34534, proxyB.foo2(new Object(), 34534.45d));
        Assert.assertEquals(0.45456d, proxyB.foo4(), 0);
        HashMap hashMap = new HashMap();
        proxyB.foo5(345349543754324L, new Long(4534986543586L), hashMap);
        Assert.assertEquals(new Long(4534986543586L), hashMap.get(345349543754324L));

        Assert.assertEquals(10, proxyB.hashCode());
        Assert.assertEquals("MyObject", proxyB.toString());
        Assert.assertTrue(proxyB.equals(null));

        // check the method call sequence
        ArrayList<IMethod> invokeList = handler.getInvokeList();
        Assert.assertEquals("foo1", invokeList.get(0).getName());
        Assert.assertEquals("foo2", invokeList.get(1).getName());
        Assert.assertEquals("foo3", invokeList.get(2).getName());
        Assert.assertEquals("foo2", invokeList.get(3).getName());
        Assert.assertEquals("foo4", invokeList.get(4).getName());
        Assert.assertEquals("foo5", invokeList.get(5).getName());
        Assert.assertEquals("hashCode", invokeList.get(6).getName());
        Assert.assertEquals("toString", invokeList.get(7).getName());
        Assert.assertEquals("equals", invokeList.get(8).getName());
    }

    public static class Handler implements ProxyInvocationHandler, InvocationHandler, Serializable {
        final private MyObject _obj = new MyObject();
        final private ArrayList<IMethod> _invokeList = new ArrayList<IMethod>();

        public Handler() {

        }

        public Object invoke(Object proxy, IMethod method, Object[] args) throws Throwable {
            _invokeList.add(method);
            return method.invoke(_obj, args);
        }

        public ArrayList<IMethod> getInvokeList() {
            return _invokeList;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return invoke(proxy, new StandardMethod(method), args);
        }
    }

    public static class MyObject implements InterfaceA, InterfaceB, Serializable {

        public void foo1() {
        }

        public int foo2(Object O, double d) {
            return (int) d;
        }

        public String foo3(long l, byte b) {
            return l + " " + b;
        }

        public double foo4() {
            return 0.45456d;
        }

        public void foo5(long l, Long ll, HashMap hm) {
            hm.put(l, ll);
        }

        @Override
        public String toString() {
            return "MyObject";
        }

        @Override
        public int hashCode() {
            return 10;
        }

        @Override
        public boolean equals(Object other) {
            return true;
        }
    }

    public interface InterfaceA {
        void foo1();

        int foo2(Object O, double d);

        String foo3(long l, byte b);
    }

    interface InterfaceB {
        int foo2(Object O, double d);

        double foo4();

        void foo5(long l, Long ll, HashMap hm);
    }
}
