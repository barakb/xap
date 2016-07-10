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

package org.openspaces.persistency.cassandra.meta.types;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for testing wheter a given type is a primite type and to obtain reference to a
 * primitive type {@link Class} instance by name.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class PrimitiveClassUtils {

    private static final Map<String, Class<?>> primitiveTypes = new HashMap<String, Class<?>>();

    static {
        primitiveTypes.put(boolean.class.getName(), boolean.class);
        primitiveTypes.put(byte.class.getName(), byte.class);
        primitiveTypes.put(char.class.getName(), char.class);
        primitiveTypes.put(short.class.getName(), short.class);
        primitiveTypes.put(int.class.getName(), int.class);
        primitiveTypes.put(long.class.getName(), long.class);
        primitiveTypes.put(float.class.getName(), float.class);
        primitiveTypes.put(double.class.getName(), double.class);
    }

    /**
     * @param typeName the type name
     * @return <code>true</code> if this name denotes a primite type
     */
    public static boolean isPrimitive(String typeName) {
        return primitiveTypes.containsKey(typeName);
    }

    /**
     * @param primitiveName the primite type name
     * @return the {@link Class} instance matching this primitive type or null of primitiveName does
     * not denote a primitve type.
     */
    public static Class<?> getPrimitive(String primitiveName) {
        return primitiveTypes.get(primitiveName);
    }
}
