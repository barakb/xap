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

package com.gigaspaces.internal.reflection.fast;

import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.IProcedure;
import com.gigaspaces.internal.reflection.ISetterMethod;

import org.objectweb.gs.asm.ClassWriter;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Creates ASM created classes that represents that provides fast "reflection" mechanism.
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ASMMethodFactory {
    final private static String CLASS_POSTFIX_NAME = "GigaspacesMethod";

    /**
     * Retrieves an IMethod or creates a new class that represents this method.
     */
    private static <T extends IProcedure> T getProcedure(ClassLoader classLoader, Method refMethod,
                                                         String name, String desc, String superClass,
                                                         boolean argsParams, boolean returnValue) throws SecurityException, NoSuchMethodException {
        final Class<?> methodClass = refMethod.getDeclaringClass();

        Method[] declaredMethods = methodClass.getDeclaredMethods();
        int methodIndex = 0;
        for (; methodIndex < declaredMethods.length; ++methodIndex)
            if (declaredMethods[methodIndex].equals(refMethod))
                break;

        String className = ASMFactoryUtils.getCreateClassNamePrefix(methodClass.getName()) + CLASS_POSTFIX_NAME + name + methodIndex;

        try {
            final ClassLoader targetClassLoader = classLoader == null ? ASMFactoryUtils.getClassTargetLoader(methodClass) : classLoader;

            String classInternalName = className.replace('.', '/'); // build internal name for ASM

            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

            cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC,
                    classInternalName, null, superClass, null);

            createCtor(cw, superClass);
            createMethod(refMethod.getDeclaringClass(), refMethod.getName(), refMethod, cw,
                    name, desc, argsParams, returnValue, refMethod.getParameterTypes());

            cw.visitEnd();

            byte[] b = cw.toByteArray();
            Class<?> definedClass = ASMFactoryUtils.defineClass(targetClassLoader, className, b);

            return (T) definedClass.getConstructor(Method.class).newInstance(refMethod);
        } catch (Exception e) {
            NoSuchMethodException ex = new NoSuchMethodException("Failed generating ASM method wrapper: " + e.toString());
            ex.initCause(e);
            throw ex;
        }
    }

    public static synchronized IMethod getMethod(Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getMethod(null, refMethod);
    }

    /**
     * Retrieves an IMethod or creates a new class that represents this method.
     */
    public static synchronized IMethod getMethod(ClassLoader classLoader, Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getProcedure(classLoader, refMethod, "internalInvoke",
                "(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;", AbstractMethod.INTERNAL_NAME, true, true);
    }

    public static synchronized IGetterMethod getGetterMethod(Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getGetterMethod(null, refMethod);
    }

    /**
     * Retrieves an IMethod or creates a new class that represents this method.
     */
    public static synchronized IGetterMethod getGetterMethod(ClassLoader classLoader, Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getProcedure(classLoader, refMethod, "internalGet",
                "(Ljava/lang/Object;)Ljava/lang/Object;", AbstractGetterMethod.INTERNAL_NAME, false, true);
    }

    public static synchronized ISetterMethod getSetterMethod(Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getSetterMethod(null, refMethod);
    }

    /**
     * Retrieves an IMethod or creates a new class that represents this method.
     */
    public static synchronized ISetterMethod getSetterMethod(ClassLoader classLoader, Method refMethod)
            throws SecurityException, NoSuchMethodException {
        return getProcedure(classLoader, refMethod, "internalSet",
                "(Ljava/lang/Object;Ljava/lang/Object;)V", AbstractSetterMethod.INTERNAL_NAME, false, false);
    }

    /**
     * Creates the class constructor which delegates the input to the super.
     */
    private static void createCtor(ClassWriter cw, String superClass) {
        MethodGenerator mv = MethodGenerator.newConstructor(cw, "(Ljava/lang/reflect/Method;)V");
        mv.start();
        mv.loadThis();
        mv.loadVariable(1);
        mv.invokeConstructor(superClass, "(Ljava/lang/reflect/Method;)V");
        mv.returnVoid();
    }

    /**
     * Creates the method invoking wrapper method.
     */
    private static void createMethod(Class clazz, String name,
                                     Method refMethod, ClassWriter cw, String methodName, String desc,
                                     boolean argsParams, boolean returnValue, Class... parameterTypes) {
        MethodGenerator mv = MethodGenerator.newVarargsMethod(cw, methodName, desc, null);

        boolean isStatic = Modifier.isStatic(refMethod.getModifiers());
        boolean isInteface = Modifier.isInterface(refMethod.getDeclaringClass().getModifiers());

        final int invokeCode;
        if (isStatic) {
            invokeCode = Opcodes.INVOKESTATIC;
        } else {
            invokeCode = isInteface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL;
            mv.castVariable(1, Type.getInternalName(clazz));
        }

        if (argsParams) {
            for (int i = 0; i < parameterTypes.length; ++i) {
                mv.loadArrayItemFromVariable(2, i);
                mv.unboxIfNeeded(parameterTypes[i]);
            }
        } else {
            for (int i = 0; i < parameterTypes.length; ++i) {
                mv.loadVariable(i + 2);
                mv.unboxIfNeeded(parameterTypes[i]);
            }
        }

        mv.invokeMethodCustom(invokeCode, Type.getInternalName(clazz), name, Type.getMethodDescriptor(refMethod));

        if (returnValue) {
            mv.prepareResult(refMethod.getReturnType());
            mv.returnObject();
        } else
            mv.returnVoid();
    }
}
