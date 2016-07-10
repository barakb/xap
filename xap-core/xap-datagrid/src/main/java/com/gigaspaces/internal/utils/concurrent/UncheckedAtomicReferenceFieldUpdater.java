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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A reflection-based utility that enables atomic updates to designated <tt>volatile</tt> reference
 * fields of designated classes.  This class is designed for use in atomic data structures in which
 * several reference fields of the same node are independently subject to atomic updates. For
 * example, a tree node might be declared as <p/>
 * <pre>
 * class Node {
 *   private volatile Node left, right;
 * <p/>
 *   private static final AtomicReferenceFieldUpdater&lt;Node, Node&gt; leftUpdater =
 *     AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "left");
 *   private static AtomicReferenceFieldUpdater&lt;Node, Node&gt; rightUpdater =
 *     AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "right");
 * <p/>
 *   Node getLeft() { return left;  }
 *   boolean compareAndSetLeft(Node expect, Node update) {
 *     return leftUpdater.compareAndSet(this, expect, update);
 *   }
 *   // ... and so on
 * }
 * </pre>
 * <p/> <p> Note that the guarantees of the <tt>compareAndSet</tt> method in this class are weaker
 * than in other atomic classes. Because this class cannot ensure that all uses of the field are
 * appropriate for purposes of atomic access, it can guarantee atomicity and volatile semantics only
 * with respect to other invocations of <tt>compareAndSet</tt> and <tt>set</tt>.
 *
 * @param <T> The type of the object holding the updatable field
 * @param <V> The type of the field
 * @author Doug Lea
 * @since 1.5
 */
public abstract class UncheckedAtomicReferenceFieldUpdater<T, V> {

    /**
     * Creates an updater for objects with the given field.  The Class arguments are needed to check
     * that reflective types and generic types match.
     *
     * @param tclass    the class of the objects holding the field.
     * @param vclass    the class of the field
     * @param fieldName the name of the field to be updated.
     * @return the updater
     * @throws IllegalArgumentException if the field is not a volatile reference type.
     * @throws RuntimeException         with a nested reflection-based exception if the class does
     *                                  not hold field or is the wrong type.
     */
    public static <U, W> AtomicReferenceFieldUpdater<U, W> newUpdater(Class<U> tclass, Class<W> vclass, String fieldName) {
        if (UnsafeHolder.isAvailable()) {
            // Currently rely on standard intrinsics implementation
            return new AtomicReferenceFieldUpdaterImpl<U, W>(tclass, vclass, fieldName);
        }
        return AtomicReferenceFieldUpdater.newUpdater(tclass, vclass, fieldName);
    }

    /**
     * Standard hotspot implementation using intrinsics
     */
    private static class AtomicReferenceFieldUpdaterImpl<T, V> extends AtomicReferenceFieldUpdater<T, V> {
        private final long offset;

        AtomicReferenceFieldUpdaterImpl(Class<T> tclass, Class<V> vclass, String fieldName) {
            Field field = null;
            Class fieldClass = null;
            int modifiers = 0;
            try {
                field = tclass.getDeclaredField(fieldName);
                modifiers = field.getModifiers();
                fieldClass = field.getType();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            if (vclass != fieldClass)
                throw new ClassCastException();

            if (!Modifier.isVolatile(modifiers))
                throw new IllegalArgumentException("Must be volatile type");

            offset = UnsafeHolder.objectFieldOffset(field);
        }


        public boolean compareAndSet(T obj, V expect, V update) {
            return UnsafeHolder.compareAndSwapObject(obj, offset, expect, update);
        }

        public boolean weakCompareAndSet(T obj, V expect, V update) {
            return UnsafeHolder.compareAndSwapObject(obj, offset, expect, update);
        }


        public void set(T obj, V newValue) {
            UnsafeHolder.putObjectVolatile(obj, offset, newValue);
        }

        public V get(T obj) {
            return (V) UnsafeHolder.getObjectVolatile(obj, offset);
        }

        public void lazySet(T obj, V newValue) {
            throw new UnsupportedOperationException();
        }
    }
}

