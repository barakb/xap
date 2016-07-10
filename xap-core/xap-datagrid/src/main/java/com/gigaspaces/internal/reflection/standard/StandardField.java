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

import com.gigaspaces.internal.reflection.IField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * A wrapper over the {@link Field} class.
 *
 * @param <T> Type of the class that contains this field type
 * @param <F> Field type.
 * @author Guy
 */
@com.gigaspaces.api.InternalApi
public class StandardField<T, F> implements IField<T, F> {

    final protected Field _field;

    public StandardField(Field field) {
        _field = field;
    }

    public Field getMember() {
        return _field;
    }

    public F get(T obj) throws IllegalArgumentException, IllegalAccessException {
        return (F) _field.get(obj);
    }

    public void set(T obj, F value) throws IllegalArgumentException, IllegalAccessException {
        _field.set(obj, value);
    }

    public String getName() {
        return _field.getName();
    }

    public Class<T> getDeclaringClass() {
        return (Class<T>) _field.getDeclaringClass();
    }

    public int getModifiers() {
        return _field.getModifiers();
    }

    public Class<F> getType() {
        return (Class<F>) _field.getType();
    }

    @Override
    public boolean equals(Object obj) {
        StandardField other = (StandardField) obj;
        return _field.equals(other._field);
    }

    @Override
    public int hashCode() {
        return _field.hashCode();
    }

    @Override
    public String toString() {
        return _field.toString();
    }

    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return _field.getAnnotation(annotationType);
    }

    public Annotation[] getAnnotations() {
        return _field.getAnnotations();
    }

    public Annotation[] getDeclaredAnnotations() {
        return _field.getDeclaredAnnotations();
    }

    public boolean isAnnotationPresent(
            Class<? extends Annotation> annotationType) {
        return _field.isAnnotationPresent(annotationType);
    }

    public boolean isAccessible() {
        return _field.isAccessible();
    }

    public void setAccessible(boolean flag) throws SecurityException {
        _field.setAccessible(flag);
    }
}
