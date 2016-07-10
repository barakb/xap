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

import org.objectweb.gs.asm.AnnotationVisitor;
import org.objectweb.gs.asm.Attribute;
import org.objectweb.gs.asm.ClassReader;
import org.objectweb.gs.asm.ClassVisitor;
import org.objectweb.gs.asm.FieldVisitor;
import org.objectweb.gs.asm.Label;
import org.objectweb.gs.asm.MethodVisitor;
import org.objectweb.gs.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;

/**
 * @author Dan Kilman
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class ConstructorPropertyNameExtractor {
    /**
     * Returns a list containing one parameter name for each argument accepted by the given
     * constructor. If the class was compiled with debugging symbols, the parameter names will match
     * those provided in the Java source code. Otherwise, a generic "arg" parameter name is
     * generated ("arg0" for the first argument, "arg1" for the second...). This method relies on
     * the constructor's class loader to locate the bytecode resource that defined its class.
     */
    public static String[] getParameterNames(Constructor<?> constructor)
            throws IOException {
        Class<?> declaringClass = constructor.getDeclaringClass();
        ClassLoader classLoader = declaringClass.getClassLoader();

        if (classLoader == null)
            throw new IllegalArgumentException("No class loader found for declaring class: " + declaringClass);

        Type declaringType = Type.getType(declaringClass);
        String constructorDescriptor = Type.getConstructorDescriptor(constructor);
        String url = declaringType.getInternalName() + ".class";

        InputStream classFileInputStream = classLoader.getResourceAsStream(url);
        if (classFileInputStream == null)
            throw new IllegalArgumentException("The constructor's class loader cannot find the bytecode that defined the constructor's class (URL: " + url + ")");

        ParameterNameExtractorClassVisitor extractor = new ParameterNameExtractorClassVisitor(constructorDescriptor);
        try {
            ClassReader classReader = new ClassReader(classFileInputStream);
            classReader.accept(extractor, 0);
        } finally {
            classFileInputStream.close();
        }

        return extractor.parameterNames;
    }

    private static class ParameterNameExtractorClassVisitor implements ClassVisitor {
        private static final String CONSTRUCTOR_METHOD_NAME = "<init>";
        private final String constructorDescription;
        private final String[] parameterNames;
        private final Type[] argumentTypes;

        ParameterNameExtractorClassVisitor(String constructorDescription) {
            this.constructorDescription = constructorDescription;
            this.argumentTypes = Type.getArgumentTypes(constructorDescription);
            this.parameterNames = new String[argumentTypes.length];
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc,
                                         String signature, String[] exceptions) {
            if (!CONSTRUCTOR_METHOD_NAME.equals(name) || !constructorDescription.equals(desc))
                return null;
            return new ParameterNameExtractorMethodVisitor(argumentTypes.length, parameterNames);
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        }

        @Override
        public void visitSource(String source, String debug) {
        }

        @Override
        public void visitOuterClass(String owner, String name, String desc) {
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            return null;
        }

        @Override
        public void visitEnd() {
        }
    }

    private static class ParameterNameExtractorMethodVisitor implements MethodVisitor {
        private final int numberOfParameters;
        private final String[] parameterNames;
        private int currentIndex = 0;

        public ParameterNameExtractorMethodVisitor(int numberOfParameters,
                                                   String[] parameterNames) {
            this.numberOfParameters = numberOfParameters;
            this.parameterNames = parameterNames;
        }

        /**
         * The assumption made here is that this method is called with the correct parameter order.
         */
        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
            if (index < 1 || // this 
                    currentIndex >= numberOfParameters || // other ctor variables
                    name == null) // might follow a long/double primitive parameter
                return;

            parameterNames[currentIndex] = name;
            currentIndex++;
        }

        @Override
        public AnnotationVisitor visitAnnotationDefault() {
            return null;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {
        }

        @Override
        public void visitCode() {
        }

        @Override
        public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
        }

        @Override
        public void visitInsn(int opcode) {
        }

        @Override
        public void visitIntInsn(int opcode, int operand) {
        }

        @Override
        public void visitVarInsn(int opcode, int var) {
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc) {
        }

        @Override
        public void visitJumpInsn(int opcode, Label label) {
        }

        @Override
        public void visitLabel(Label label) {
        }

        @Override
        public void visitLdcInsn(Object cst) {
        }

        @Override
        public void visitIincInsn(int var, int increment) {
        }

        @Override
        public void visitTableSwitchInsn(int min, int max, Label dflt, Label[] labels) {
        }

        @Override
        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels) {
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {
        }

        @Override
        public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
        }

        @Override
        public void visitLineNumber(int line, Label start) {
        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {
        }

        @Override
        public void visitEnd() {
        }
    }

}
