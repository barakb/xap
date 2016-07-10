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

package com.gigaspaces.internal.reflection.fast.proxy;

import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.MethodHolder;
import com.gigaspaces.internal.reflection.ProxyInvocationHandler;
import com.gigaspaces.internal.reflection.fast.ASMFactoryUtils;
import com.gigaspaces.internal.reflection.fast.MethodGenerator;

import org.objectweb.gs.asm.ClassWriter;
import org.objectweb.gs.asm.FieldVisitor;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicInteger;

@com.gigaspaces.api.InternalApi
public class ProxyFactory {
    final static private String OBJECT_INTERNALNAME = Type.getInternalName(Object.class);
    final static private String CTOR_DESC = Type.getMethodDescriptor(Type.VOID_TYPE, new Type[]{Type.getType(ProxyInvocationHandler.class), Type.BOOLEAN_TYPE});

    final private static AtomicInteger _proxyID = new AtomicInteger();
    final private static ProxyCache _proxyCache = new ProxyCache();

    /**
     * Returns an instance of a class for the specified interfaces that dispatches method
     * invocations to the specified invocation handler.
     *
     * @param handler    the invocation handler to dispatch method invocations to
     * @param loader     the class loader to define the proxy class
     * @param interfaces the list of interfaces for the proxy class to implement
     * @return a proxy instance with the specified invocation handler of a proxy class that is
     * defined by the specified class loader and that implements the specified interfaces
     * @throws IllegalArgumentException if any of the restrictions on the parameters that may be
     *                                  passed to <code>getProxyClass</code> are violated
     * @throws NullPointerException     if the <code>interfaces</code> array argument or any of its
     *                                  elements are <code>null</code>, or if the invocation
     *                                  handler, <code>h</code>, is <code>null</code>
     */
    public static Object newProxyInstance(ClassLoader loader, Class<?>[] interfaces,
                                          ProxyInvocationHandler handler, boolean allowCache) throws IllegalArgumentException {
        if (loader == null)
            loader = AbstractProxy.class.getClassLoader();

        Class definedClass = _proxyCache.findInCache(loader, interfaces);

        try {
            if (definedClass == null) {
                String packageName = getPackageName(interfaces);
                String className = packageName + ".$GSProxy" + _proxyID.getAndIncrement();
                String classInternalName = className.replace('.', '/'); // build internal name for ASM

                String[] interfacesName = new String[interfaces.length];
                for (int i = 0; i < interfaces.length; ++i) {
                    interfacesName[i] = Type.getInternalName(interfaces[i]);
                }
                ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
                cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER, classInternalName,
                        null, AbstractProxy.INTERNAL_NAME, interfacesName);

                MethodHolder[] uniqueMethods = AbstractProxy.getUniqueMethodHolders(interfaces);

                createStaticCtor(cw, classInternalName);
                createCtor(cw);
                createMethods(cw, uniqueMethods, classInternalName);

                cw.visitEnd();

                byte[] b = cw.toByteArray();

                definedClass = ASMFactoryUtils.defineClass(loader, className, b);

                _proxyCache.add(loader, interfaces, definedClass);
            }
            Constructor ctor = definedClass.getConstructor(ProxyInvocationHandler.class, boolean.class);
            AbstractProxy proxy = (AbstractProxy) ctor.newInstance(handler, allowCache);
            return proxy;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Find the best match package name according to the provided interfaces. If one of the
     * interfaces is not public we try to use it. Other wise uses a default one.
     */
    private static String getPackageName(Class<?>[] interfaces) {
        String notPublicPackage = null;
        for (Class<?> inter : interfaces) {
            if (!Modifier.isPublic(inter.getModifiers())) {
                if (notPublicPackage == null) {
                    notPublicPackage = inter.getPackage().getName();
                } else if (!notPublicPackage.equals(inter.getPackage().getName())) {
                    throw new IllegalArgumentException("non-public interfaces from different packages");
                }
            }
        }
        return notPublicPackage == null ? "com.gigaspaces.reflect" : notPublicPackage;
    }

    /**
     * Creates method for the interfaces
     */
    private static void createMethods(ClassWriter cw, MethodHolder[] methods, String classInternalName) {
        for (int j = 0; j < methods.length; ++j) {
            final MethodHolder method = methods[j];
            Class<?>[] exceptionTypes = method.getMethod().getExceptionTypes();
            String[] exceptions = new String[exceptionTypes.length];
            for (int i = 0; i < exceptionTypes.length; ++i)
                exceptions[i] = Type.getInternalName(exceptionTypes[i]);

            // TODO set signature?
            MethodGenerator mv = MethodGenerator.newVarargsMethod(cw, method.getName(), method.getMethodDescriptor(),
                    exceptions);
            mv.start();

            // _handler....
            mv.loadThis();
            mv.loadField(AbstractProxy.INTERNAL_NAME, "_handler", "L" + ProxyInvocationHandler.INTERNAL_NAME + ";");

            // this
            mv.loadThis();

            // _methods[j]
            mv.loadStaticField(classInternalName, "_methods", IMethod.ARRAY_DESCRIPTOR_NAME);
            mv.loadConstant(j);
            mv.loadArrayItem();

            // new Object[]
            final Class<?>[] parameterTypes = method.getMethod().getParameterTypes();
            mv.newArray(OBJECT_INTERNALNAME, parameterTypes.length);


            // {arg1,arg2,...}
            int argPos = 1;
            for (int i = 0; i < parameterTypes.length; ++i) {
                mv.dup();
                mv.loadConstant(i); // array index to save
                // argument load and cast to Object
                argPos += mv.loadVariable(parameterTypes[i], argPos);
                mv.storeArrayItem();
            }

            mv.invokeMethodCustom(Opcodes.INVOKEINTERFACE, ProxyInvocationHandler.INTERNAL_NAME,
                    "invoke",
                    "(Ljava/lang/Object;" + IMethod.DESCRIPTOR_NAME + "[Ljava/lang/Object;)Ljava/lang/Object;");

            mv.returnResult(method.getMethod().getReturnType());
        }
    }

    private static void createStaticCtor(ClassWriter cw, String className) {
        //final static private IMethod[] _methods;
        FieldVisitor fv = cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL | Opcodes.ACC_STATIC,
                "_methods", IMethod.ARRAY_DESCRIPTOR_NAME, null, null);
        fv.visitEnd();

        //static{
        //   _methods = AbstractProxy.getIMethods(.$GSProxy1.class);
        //}
        MethodGenerator mv = MethodGenerator.newStaticConstructor(cw, "()V");
        mv.start();
        mv.loadConstant(Type.getType("L" + className + ";"));
        mv.invokeStaticMethod(AbstractProxy.INTERNAL_NAME, "getIMethods",
                "(Ljava/lang/Class;)[Lcom/gigaspaces/internal/reflection/IMethod;");
        mv.storeStaticField(className, "_methods", IMethod.ARRAY_DESCRIPTOR_NAME);
        mv.returnVoid();
    }

    /**
     * Creates the class constructor which delegates to the super.
     */
    private static void createCtor(ClassWriter cw) {

        MethodGenerator mv = MethodGenerator.newConstructor(cw, CTOR_DESC);

        mv.start();
        mv.loadThis();
        mv.loadVariable(1);
        mv.loadVariableInt(2);
        mv.invokeConstructor(AbstractProxy.INTERNAL_NAME, CTOR_DESC);

        mv.returnVoid();
    }
}
