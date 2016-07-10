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

package com.j_spaces.kernel;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Flat sizeof() for java Objects.
 *
 * @author Guy Korland
 */
@com.gigaspaces.api.InternalApi
public class Sizeof {
    private static final int SZ_REF;

    static {
        int size = 4;
        try {
            size = Integer.parseInt(System.getProperty("sun.arch.data.model")) / 8;
        } catch (Exception e) {
        }
        SZ_REF = size;
    }

    public static int sizeof(boolean b) {
        return 1;
    }

    public static int sizeof(byte b) {
        return 1;
    }

    public static int sizeof(char c) {
        return 2;
    }

    public static int sizeof(short s) {
        return 2;
    }

    public static int sizeof(int i) {
        return 4;
    }

    public static int sizeof(long l) {
        return 8;
    }

    public static int sizeof(float f) {
        return 4;
    }

    public static int sizeof(double d) {
        return 8;
    }

    private static int size_inst(Class c) {
        Field flds[] = c.getDeclaredFields();
        int sz = 0;

        for (int i = 0; i < flds.length; i++) {
            Field f = flds[i];
            if (!c.isInterface() &&
                    (f.getModifiers() & Modifier.STATIC) != 0)
                continue;
            sz += size_prim(f.getType());
        }

        if (c.getSuperclass() != null)
            sz += size_inst(c.getSuperclass());

        Class cv[] = c.getInterfaces();
        for (int i = 0; i < cv.length; i++)
            sz += size_inst(cv[i]);

        return sz;
    }

    private static int size_prim(Class t) {
        if (t == Boolean.TYPE)
            return 1;
        if (t == Byte.TYPE)
            return 1;
        if (t == Character.TYPE)
            return 2;
        if (t == Short.TYPE)
            return 2;
        if (t == Integer.TYPE)
            return 4;
        if (t == Long.TYPE)
            return 8;
        if (t == Float.TYPE)
            return 4;
        if (t == Double.TYPE)
            return 8;
        if (t == Void.TYPE)
            return 0;

        return SZ_REF;
    }

    private static int size_arr(Object obj, Class c) {
        Class ct = c.getComponentType();
        int len = Array.getLength(obj);

        if (ct.isPrimitive()) {
            return len * size_prim(ct);
        }
        int sz = 0;
        for (int i = 0; i < len; i++) {
            sz += SZ_REF;
            Object obj2 = Array.get(obj, i);
            if (obj2 == null)
                continue;
            Class c2 = obj2.getClass();
            if (!c2.isArray())
                continue;
            sz += size_arr(obj2, c2);
        }
        return sz;
    }

    public static int sizeof(Object obj) {
        if (obj == null)
            return 0;

        Class c = obj.getClass();

        if (c.isArray())
            return size_arr(obj, c);

        return size_inst(c);
    }

}
