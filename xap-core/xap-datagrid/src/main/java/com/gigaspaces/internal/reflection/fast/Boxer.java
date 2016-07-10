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

import org.objectweb.gs.asm.Type;

/**
 * Box primitive types. Created to support java 1.4 and java 1.5.
 *
 * @author guy
 * @since 6.5.1
 */
@com.gigaspaces.api.InternalApi
public class Boxer {

    final static public String INTERNAL_NAME = Type.getInternalName(Boxer.class);

    public static Object box(boolean v) {
        return v;
    }

    public static Object box(byte v) {
        return v;
    }

    public static Object box(char v) {
        return v;
    }

    public static Object box(short v) {
        return v;
    }

    public static Object box(int v) {
        return v;
    }

    public static Object box(long v) {
        return v;
    }

    public static Object box(float v) {
        return v;
    }

    public static Object box(double v) {
        return v;
    }

}
