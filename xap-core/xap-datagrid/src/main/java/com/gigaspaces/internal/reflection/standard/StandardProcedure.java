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

package com.gigaspaces.internal.reflection.standard;

import com.gigaspaces.internal.reflection.IProcedure;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Provides a wrapper over the standard method reflection
 *
 * @author GuyK
 * @since 6.5.1
 */
abstract public class StandardProcedure<T> implements IProcedure<T> {

    final protected Method _method;

    private transient Class<?>[] parameterTypes;

    private transient Class[] exceptionTypes;

    public StandardProcedure(Method method) {
        _method = method;
        if (!_method.isAccessible())
            _method.setAccessible(true);
    }

    public Method getMember() {
        return _method;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        return _method.equals(((StandardProcedure) obj)._method);
    }

    @Override
    public int hashCode() {
        return _method.hashCode();
    }

    @Override
    public String toString() {
        return _method.toString();
    }

    public Class<?> getDeclaringClass() {
        return _method.getDeclaringClass();
    }

    public String getName() {
        return _method.getName();
    }

    public Class<?> getReturnType() {
        return _method.getReturnType();
    }

    public Class<?>[] getParameterTypes() {
        // we cache the parameter types since every call to it clones the array
        if (parameterTypes == null) {
            parameterTypes = _method.getParameterTypes();
        }
        return parameterTypes;
    }

    public int getModifiers() {
        return _method.getModifiers();
    }

    public Class[] getExceptionTypes() {
        if (exceptionTypes == null) {
            exceptionTypes = _method.getExceptionTypes();
        }
        return exceptionTypes;
    }

    public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
        return _method.getAnnotation(annotationClass);
    }

    public Method getMethod() {
        return _method;
    }

    public Annotation[] getAnnotations() {
        return _method.getAnnotations();
    }

    public Annotation[] getDeclaredAnnotations() {
        return _method.getDeclaredAnnotations();
    }

    public boolean isAnnotationPresent(
            Class<? extends Annotation> annotationType) {
        return _method.isAnnotationPresent(annotationType);
    }

    public boolean isAccessible() {
        return _method.isAccessible();
    }

    public void setAccessible(boolean flag) throws SecurityException {
        _method.setAccessible(flag);
    }


}
