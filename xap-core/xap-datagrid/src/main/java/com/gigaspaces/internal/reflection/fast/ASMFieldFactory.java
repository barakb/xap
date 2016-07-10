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

import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.standard.StandardField;

import org.objectweb.gs.asm.ClassVisitor;
import org.objectweb.gs.asm.ClassWriter;
import org.objectweb.gs.asm.Opcodes;
import org.objectweb.gs.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;


/**
 * Creates ASM created classes that represents that provides fast fields "reflection" mechanism.
 *
 * @author AssafR
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class ASMFieldFactory {

    final private static String CLASS_POSTFIX_NAME = "GigaspacesField";
    final private static String STANDARD_FIELD_INTERNAL_NAME = Type.getInternalName(StandardField.class);

    public static IField getField(Field refField) throws NoSuchFieldException {
        Class declaringClass = refField.getDeclaringClass();
        String ownerClassName = declaringClass.getName();

        Field[] declaredFields = declaringClass.getDeclaredFields();
        int fieldIndex = 0;
        for (; fieldIndex < declaredFields.length; ++fieldIndex) {
            if (declaredFields[fieldIndex].equals(refField))
                break;
        }

        String className = ASMFactoryUtils.getCreateClassNamePrefix(ownerClassName) + CLASS_POSTFIX_NAME + fieldIndex;

        try {
            final ClassLoader targetClassLoader = ASMFactoryUtils.getClassTargetLoader(declaringClass);
            String classInternalName = className.replace('.', '/'); // build internal name for ASM

            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER, classInternalName,
                    null, STANDARD_FIELD_INTERNAL_NAME, null);

            createConstructor(cw);
            createGetMethod(cw, declaringClass, refField);
            createSetMethod(cw, declaringClass, refField);
            cw.visitEnd();

            byte[] b = cw.toByteArray();

            Class definedClass = ASMFactoryUtils.defineClass(targetClassLoader, className, b);
            Constructor ctor = definedClass.getConstructor(Field.class);
            return (IField) ctor.newInstance(refField);
        } catch (Exception e) {
            NoSuchFieldException err = new NoSuchFieldException("Failed generating ASM field wrapper: " + e.toString());
            err.initCause(e);
            throw err;
        }
    }

    private static void createConstructor(ClassVisitor cw) {
        MethodGenerator mv = MethodGenerator.newConstructor(cw, "(Ljava/lang/reflect/Field;)V");
        mv.start();
        mv.loadThis();
        mv.loadVariable(1);
        mv.invokeConstructor(STANDARD_FIELD_INTERNAL_NAME, "(Ljava/lang/reflect/Field;)V");
        mv.returnVoid();
    }

    private static void createGetMethod(ClassVisitor cw, Class entryClass, Field field) {
        MethodGenerator mv = MethodGenerator.newMethod(cw, "get", "(Ljava/lang/Object;)Ljava/lang/Object;",
                new String[]{"java/lang/IllegalArgumentException", "java/lang/IllegalAccessException"});
        mv.start();
        mv.castVariable(1, Type.getInternalName(entryClass));
        mv.loadField(entryClass, field);
        mv.boxIfNeeded(field.getType());
        mv.returnObject();
    }

    private static void createSetMethod(ClassVisitor cw, Class entryClass, Field field) {
        MethodGenerator mv = MethodGenerator.newMethod(cw, "set", "(Ljava/lang/Object;Ljava/lang/Object;)V",
                new String[]{"java/lang/IllegalArgumentException", "java/lang/IllegalAccessException"});
        mv.start();
        mv.castVariable(1, Type.getInternalName(entryClass));
        mv.loadVariable(2);
        //mv.castAndUnboxIfNeeded(field.getType());
        mv.unboxIfNeeded(field.getType());
        mv.storeField(entryClass, field);
        mv.returnVoid();
    }
}
