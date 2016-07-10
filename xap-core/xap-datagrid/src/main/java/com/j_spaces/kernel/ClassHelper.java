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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Helper for setting/getting any public properties.
 *
 * @author Gershon Diner
 * @version 4.0
 */
public final class ClassHelper {


    /**
     * A map of objectified primitive types, and their corresponding primitive types
     */
    private static final Class[][] TYPES = {
            {Boolean.class, boolean.class}, {Byte.class, byte.class},
            {Short.class, short.class}, {Character.class, char.class},
            {Integer.class, int.class}, {Long.class, long.class},
            {Float.class, float.class}, {Double.class, double.class}};


    /**
     * Prevent construction of utility class
     */
    private ClassHelper() {
    }

    /**
     * Helper to return a method given its name and a list of arguments
     *
     * @param type the class to locate the method on
     * @param name the method name
     * @param args the list of arguments to match against, or null if the method takes no arguments
     * @return the method matching the name and list of arguments
     * @throws NoSuchMethodException if the method cannot be found
     */
    public static Method getMethod(Class type, String name, Object[] args) throws NoSuchMethodException {
        boolean containsNull = false;
        Class<?>[] types = null;
        if (args == null)
            types = new Class[]{};
        else {
            types = new Class[args.length];
            for (int i = 0; i < args.length; ++i) {
                if (args[i] != null)
                    types[i] = args[i].getClass();
                else
                    containsNull = true;
            }
        }

        Method method = null;
        if (!containsNull) {
            try {
                // try a direct lookup
                method = type.getMethod(name, types);
            } catch (NoSuchMethodException ignore) {
                if (types.length != 0) {
                    // try converting any objectified types to primitives
                    Class<?>[] converted = new Class[types.length];
                    if (args != null)
                        for (int i = 0; i < args.length; ++i)
                            converted[i] = getPrimitiveType(args[i].getClass());
                    try {
                        method = type.getMethod(name, types);
                    } catch (NoSuchMethodException ignore2) {
                        // no-op
                    }
                }
            }
        }
        if (method == null) {
            // try and find it iteratively, using the class' public declared
            // methods
            Method[] methods = type.getDeclaredMethods();
            for (int i = 0; i < methods.length; ++i) {
                if (Modifier.isPublic(methods[i].getModifiers())
                        && methods[i].getName().equals(name)) {
                    if (checkParameters(methods[i], types)) {
                        method = methods[i];
                        break;
                    }
                }
            }
            if (method == null) {
                // try and find it iteratively, using the MUCH slower
                // getMethods() method
                methods = type.getMethods();
                for (int i = 0; i < methods.length; ++i) {
                    if (methods[i].getName().equals(name)) {
                        if (checkParameters(methods[i], types)) {
                            method = methods[i];
                            break;
                        }
                    }
                }
            }
            if (method == null) {
                String msg = "No method found for name=" + name
                        + ", argument types=(";
                if (args != null && args.length > 0) {
                    for (int i = 0; i < args.length; ++i) {
                        if (i > 0) {
                            msg += ", ";
                        }
                        if (args[i] != null) {
                            msg += args[i].getClass().getName();
                        } else {
                            msg += "Object";
                        }
                    }
                }
                msg += ")";

                throw new NoSuchMethodException(msg);
            }
        }
        return method;
    }

    /**
     * Helper to return the primitive type associated with an object wrapper type
     *
     * @param wrapper the object wrapper class
     * @return the associated primitive type, or the wrapper type, if there is no associated
     * primitive type
     */
    public static Class getPrimitiveType(Class wrapper) {
        Class result = null;
        for (int i = 0; i < TYPES.length; ++i) {
            if (wrapper.equals(TYPES[i][0])) {
                result = TYPES[i][1];
                break;
            }
        }

        return (result != null) ? result : wrapper;
    }

    /**
     * Helper to return the primitive type name associated with an object wrapper type
     *
     * @param wrapper the object wrapper class
     * @return the associated primitive type name, or the wrapper type name, if there is no
     * associated primitive type. Array types are returned in the form '<class>[]'
     */
    public static String getPrimitiveName(Class wrapper) {
        String result = null;
        Class type = getPrimitiveType(wrapper);
        if (type.isArray()) {
            result = type.getComponentType().getName() + "[]";
        } else {
            result = type.getName();
        }
        return result;
    }

    /**
     * Helper to determine if a list of argument types match that of a method
     *
     * @param method the method to check
     * @param types  the argument types
     * @return <code>true</code> if the argument types are compatible
     */
    private static boolean checkParameters(Method method, Class[] types) {
        boolean result = true;
        Class[] parameters = method.getParameterTypes();
        if (parameters.length != types.length) {
            result = false;
        } else {
            for (int i = 0; i < parameters.length; ++i) {
                Class parameter = parameters[i];
                if (types[i] == null) {
                    if (parameter.isPrimitive()) {
                        result = false;
                        break;
                    }
                } else if (!parameter.isAssignableFrom(types[i])
                        && !parameter.isAssignableFrom(
                        getPrimitiveType(types[i]))) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

}
