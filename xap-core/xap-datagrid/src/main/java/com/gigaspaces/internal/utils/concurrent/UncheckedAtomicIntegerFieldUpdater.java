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


package com.gigaspaces.internal.utils.concurrent;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A reflection-based utility that enables atomic updates to designated <tt>volatile int</tt> fields
 * of designated classes. This class is designed for use in atomic data structures in which several
 * fields of the same node are independently subject to atomic updates. <p/> <p> Note that the
 * guarantees of the <tt>compareAndSet</tt> method in this class are weaker than in other atomic
 * classes. Because this class cannot ensure that all uses of the field are appropriate for purposes
 * of atomic access, it can guarantee atomicity and volatile semantics only with respect to other
 * invocations of <tt>compareAndSet</tt> and <tt>set</tt>.
 *
 * @param <T> The type of the object holding the updatable field
 * @author Doug Lea
 * @since 1.5
 */

// Gigaspaces: Removed the tClass.isInstance check 
public abstract class UncheckedAtomicIntegerFieldUpdater<T> {
    /**
     * Creates an updater for objects with the given field.  The Class argument is needed to check
     * that reflective types and generic types match.
     *
     * @param tclass    the class of the objects holding the field
     * @param fieldName the name of the field to be updated.
     * @return the updater
     * @throws IllegalArgumentException if the field is not a volatile integer type.
     * @throws RuntimeException         with a nested reflection-based exception if the class does
     *                                  not hold field or is the wrong type.
     */
    public static <U> AtomicIntegerFieldUpdater<U> newUpdater(Class<U> tclass, String fieldName) {
        if (UnsafeHolder.isAvailable()) {
            return new AtomicIntegerFieldUpdaterImpl<U>(tclass, fieldName);
        }
        return AtomicIntegerFieldUpdater.newUpdater(tclass, fieldName);
    }

    /**
     * Standard hotspot implementation using intrinsics
     */
    private static class AtomicIntegerFieldUpdaterImpl<T> extends AtomicIntegerFieldUpdater<T> {
        private final long offset;

        AtomicIntegerFieldUpdaterImpl(Class<T> tclass, String fieldName) {
            Field field = null;
            int modifiers = 0;
            try {
                field = tclass.getDeclaredField(fieldName);
                modifiers = field.getModifiers();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            Class fieldt = field.getType();
            if (fieldt != int.class)
                throw new IllegalArgumentException("Must be integer type");

            if (!Modifier.isVolatile(modifiers))
                throw new IllegalArgumentException("Must be volatile type");

            offset = UnsafeHolder.objectFieldOffset(field);
        }

        public boolean compareAndSet(T obj, int expect, int update) {
            return UnsafeHolder.compareAndSwapInt(obj, offset, expect, update);
        }

        public boolean weakCompareAndSet(T obj, int expect, int update) {
            return UnsafeHolder.compareAndSwapInt(obj, offset, expect, update);
        }

        public void set(T obj, int newValue) {
            UnsafeHolder.putIntVolatile(obj, offset, newValue);
        }

        public final int get(T obj) {
            return UnsafeHolder.getIntVolatile(obj, offset);
        }

        public void lazySet(T obj, int newValue) {
            throw new UnsupportedOperationException();
        }
    }
}
