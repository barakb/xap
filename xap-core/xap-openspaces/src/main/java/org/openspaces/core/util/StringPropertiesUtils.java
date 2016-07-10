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

package org.openspaces.core.util;

import com.gigaspaces.internal.utils.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringPropertiesUtils {

    public static void store(Map<String, String> properties, OutputStream out, String comments) throws IOException {
        Properties properties2 = new Properties();
        properties2.putAll(properties);
        properties2.store(out, comments);
    }

    public static Map<String, String> load(InputStream in) throws IOException {

        Properties properties = new Properties();
        properties.load(in);
        return convertPropertiesToMapStringString(properties);
    }

    public static Map<String, String> convertPropertiesToMapStringString(Properties properties2) {
        Map<String, String> properties = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : properties2.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return properties;
    }

    public static Map<String, String> load(String filename) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File(filename));
        try {
            return load(fileInputStream);
        } finally {
            fileInputStream.close();
        }
    }

    public static int getInteger(Map<String, String> properties, String key, int defaultValue) throws NumberFormatException {

        int intValue = defaultValue;

        Object value = properties.get(key);
        if (value != null) {
            intValue = Integer.valueOf(value.toString());
        }

        return intValue;
    }

    public static int getIntegerIgnoreExceptions(Map<String, String> properties, String key, int defaultValue) {
        int intValue = defaultValue;
        try {
            intValue = getInteger(properties, key, defaultValue);
        } catch (NumberFormatException e) {
            //fallthrough
        }
        return intValue;
    }

    public static void putLong(Map<String, String> properties, String key, long value) {
        properties.put(key, String.valueOf(value));
    }

    public static long getLong(Map<String, String> properties, String key, long defaultValue) throws NumberFormatException {

        long longValue = defaultValue;

        Object value = properties.get(key);
        if (value != null) {
            longValue = Long.valueOf(value.toString());
        }

        return longValue;
    }

    public static long getLongIgnoreExceptions(Map<String, String> properties, String key, long defaultValue) {
        long longValue = defaultValue;
        try {
            longValue = getLong(properties, key, defaultValue);
        } catch (NumberFormatException e) {
            //fallthrough
        }
        return longValue;
    }

    public static void putDouble(Map<String, String> properties, String key, double value) {
        properties.put(key, String.valueOf(value));
    }

    public static double getDouble(Map<String, String> properties, String key, double defaultValue) throws NumberFormatException {

        double doubleValue = defaultValue;

        Object value = properties.get(key);
        if (value != null) {
            doubleValue = Double.valueOf(value.toString());
        }

        return doubleValue;
    }

    public static double getDoubleIgnoreExceptions(Map<String, String> properties, String key, double defaultValue) {
        double doubleValue = defaultValue;
        try {
            doubleValue = getDouble(properties, key, defaultValue);
        } catch (NumberFormatException e) {
            //fallthrough
        }
        return doubleValue;
    }

    /**
     * Concatenates the specified array into a combined string using the specified separator and
     * puts the result as a value into the specified properties with the specified key. The values
     * in the array must not contain the separator otherwise an IllegalArgumentException is raised.
     * Null or empty arrays will remove the key from the properties map
     */
    public static void putArray(Map<String, String> properties, String key, String[] array, String separator) {
        if (array == null || array.length == 0) {
            properties.remove(key);
        } else {
            StringBuilder concat = new StringBuilder();
            for (int i = 0; i < array.length; i++) {
                String value = array[i];
                if (value != null && value.length() > 0) {
                    if (value.contains(separator)) {
                        throw new IllegalArgumentException("array contains an element '" + value + "' that contains the separator '" + separator + "'");
                    }
                    concat.append(value);
                    if (i < array.length - 1) {
                        concat.append(separator);
                    }
                }
            }
            properties.put(key, concat.toString());
        }
    }

    public static String[] getArray(Map<String, String> properties, String key, String separator, String[] defaultValue) {
        String[] array;
        String value = properties.get(key);
        if (value == null) {
            array = defaultValue;
        } else {
            array = value.split(java.util.regex.Pattern.quote(separator));
        }
        return array;
    }

    public static Set<String> getSet(Map<String, String> properties, String key, String separator, Set<String> defaultValue) {
        String[] defaultArray = defaultValue == null ? null : defaultValue.toArray(new String[0]);
        String[] array = getArray(properties, key, separator, defaultArray);
        Set<String> set = null;
        if (array != null) {
            set = new HashSet<String>(Arrays.asList(array));
        }
        return set;
    }

    public static void putSet(Map<String, String> properties, String key, Set<String> value, String separator) {
        if (value.isEmpty()) {
            //same behavior as map, different from array behavior
            properties.remove(key);
        } else {
            putArray(properties, key, value.toArray(new String[0]), separator);
        }
    }

    /**
     * Concatenates the specified array into a combined string using the space separator and puts
     * the result as a value into the specified properties with the specified key. If the values in
     * the array contains whitespace it is enclosed with " or ' characters
     */
    public static void putArgumentsArray(Map<String, String> properties, String key, String[] array) {
        StringBuilder concat = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            String value = array[i];
            if (value != null && value.length() > 0) {
                if (value.contains(" ")) {
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        if (value.substring(1, value.length() - 1).contains("\"")) {
                            throw new IllegalArgumentException("Argument " + value + " contains both a whitespace and a \" character.");
                        }
                    } else if (value.startsWith("'") && value.endsWith("'")) {
                        if (value.substring(1, value.length() - 1).contains("'")) {
                            throw new IllegalArgumentException("Argument " + value + " contains both a whitespace and a ' character.");
                        }
                    } else if (!value.contains("\"")) {
                        value = "\"" + value + "\"";
                    } else if (!value.contains("'")) {
                        value = "'" + value + "'";
                    } else {
                        throw new IllegalArgumentException("Argument " + value + " contains both a whitespace and \" and '");
                    }
                }
                concat.append(value);
                if (i < array.length - 1) {
                    concat.append(' ');
                }
            }
        }
        properties.put(key, concat.toString());
    }

    public static String[] getArgumentsArray(Map<String, String> properties, String key, String[] defaultValue) {
        String[] array = defaultValue;
        String value = properties.get(key);
        if (value != null) {
            List<String> matchList = new ArrayList<String>();
            Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
            Matcher regexMatcher = regex.matcher(value);
            while (regexMatcher.find()) {
                if (regexMatcher.group(1) != null) {
                    // Add double-quoted string without the quotes
                    matchList.add(regexMatcher.group(1));
                } else if (regexMatcher.group(2) != null) {
                    // Add single-quoted string without the quotes
                    matchList.add(regexMatcher.group(2));
                } else {
                    // Add unquoted word
                    matchList.add(regexMatcher.group());
                }
            }
            array = matchList.toArray(new String[matchList.size()]);
        }
        return array;
    }

    public static void putInteger(Map<String, String> properties, String key, int value) {
        properties.put(key, String.valueOf(value));
    }

    public static void putBoolean(Map<String, String> properties, String key, boolean value) {
        properties.put(key, String.valueOf(value));
    }

    public static boolean getBoolean(Map<String, String> properties, String key, boolean defaultValue) {
        boolean booleanValue = defaultValue;
        String value = properties.get(key);

        //don't use Boolean.valueOf() since it always defaults to false 
        if (value != null) {
            if (value.equalsIgnoreCase("true")) {
                booleanValue = true;
            } else if (value.equalsIgnoreCase("false")) {
                booleanValue = false;
            } else {
                throw new IllegalArgumentException(key + " must be either true or false. The value " + key + " is illegal.");
            }
        }
        return booleanValue;
    }

    public static Map<String, String> getMap(Map<String, String> properties, String keyPrefix, Map<String, String> defaultValue) {
        Map<String, String> value = new HashMap<String, String>();
        for (Entry<String, String> pair : properties.entrySet()) {
            String key = pair.getKey();
            if (key.startsWith(keyPrefix)) {
                String newKey = key.substring(keyPrefix.length());
                value.put(newKey, pair.getValue());
            }
        }
        if (value.size() == 0) {
            value = defaultValue;
        }
        return value;

    }

    /**
     * Puts the map as a value by adding the specified prefix to each key If value is null the
     * object is removed from properties.
     */
    public static void putMap(Map<String, String> properties, String keyPrefix, Map<String, String> value) {
        if (properties == value) {
            throw new IllegalArgumentException("properties and value must be different objects");
        }

        // delete old properties starting with the key prefix
        Set<String> keysToDelete = new HashSet<String>();
        for (String key : properties.keySet()) {
            if (key.toString().startsWith(keyPrefix)) {
                keysToDelete.add(key);
            }
        }

        for (String key : keysToDelete) {
            properties.remove(key);
        }
        if (value != null) {
            // add new properties with the new key prefix
            for (Entry<String, String> pair : value.entrySet()) {
                properties.put(keyPrefix + pair.getKey(), pair.getValue());
            }
        }
    }

    public static String toString(Map<String, String> properties) {
        //sort and print
        return new TreeMap<String, String>(properties).toString();
    }

    public static void putKeyValuePairs(
            Map<String, String> properties, String key, Map<String, String> value,
            String pairSeperator, String keyValueSeperator) {

        putArray(properties, key, StringUtils.convertKeyValuePairsToArray(value, keyValueSeperator), pairSeperator);
    }

    public static Map<String, String> getKeyValuePairs(Map<String, String> properties, String key, String pairSeperator, String keyValueSeperator, Map<String, String> defaultValue) {
        String[] pairs = getArray(properties, key, pairSeperator, StringUtils.convertKeyValuePairsToArray(defaultValue, keyValueSeperator));
        return StringUtils.convertArrayToKeyValuePairs(pairs, keyValueSeperator);
    }

    /**
     * Puts an object that has a constructor that accepts Map<String,String> as a single argument
     * and has a getProperties method which returns a Map<String,String> If object is null it is
     * removed from the map
     */
    @SuppressWarnings("unchecked")
    public static void putConfig(Map<String, String> properties, String key, Object object) {
        String classKey = key + ".class";
        String valuesKey = key + ".values.";
        if (object == null) {
            properties.remove(classKey);
            putMap(properties, valuesKey, null);
            return;
        }

        Object objectProperties;
        Class<?> clazz = object.getClass();
        try {
            Method getPropertiesMethod = clazz.getMethod("getProperties");
            objectProperties = getPropertiesMethod.invoke(object);
        } catch (SecurityException e) {
            throw new RuntimeException("Failed to verify getProperties method", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to verify getProperties method", e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Failed to verify getProperties method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to verify getProperties method", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to verify getProperties method", e);
        }

        if (!(properties instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(key + " value type (" + clazz + ") does not have a getProperties() method that returns a Map");
        }
        try {
            if (clazz.getConstructor(Map.class) == null) {
                throw new IllegalArgumentException(key + " value type (" + clazz + ") does not have a constructor that accepts a String");
            }
        } catch (SecurityException e) {
            throw new RuntimeException("Failed to verify constructor that accepts a map", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to verify constructor that accepts a map", e);
        }

        properties.put(classKey, clazz.getName());
        putMap(properties, valuesKey, (Map<String, String>) objectProperties);
    }

    /**
     * Gets an object that has a constructor that accepts Map<String,String> as a single argument
     */
    public static Object getConfig(Map<String, String> properties, String key, Object defaultValue) {
        Object value = defaultValue;
        String className = properties.get(key + ".class");
        if (className != null) {
            Map<String, String> objectProperties = getMap(properties, key + ".values.", new HashMap<String, String>());
            if (objectProperties != null) {
                try {
                    Class<?> clazz = Class.forName(className);
                    value = clazz.getConstructor(Map.class).newInstance(objectProperties);
                } catch (InstantiationException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                }
            }
        }
        return value;
    }

    /**
     * Puts an object that has a constructor that accepts String as a single argument
     */
    public static void putStringWrapperObject(Map<String, String> properties, String key, Object value) {
        try {
            if (value.getClass().getConstructor(String.class) == null) {
                throw new IllegalArgumentException(key + " value type (" + value.getClass() + ") does not have a constructor that accepts a String");
            }
        } catch (SecurityException e) {
            throw new RuntimeException("Failed to verify " + value.getClass().getName() + " class type", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to verify " + value.getClass().getName() + " class type", e);
        }
        properties.put(key + ".class", value.getClass().getName());
        properties.put(key + ".value", value.toString());
    }

    /**
     * Gets an object that has a constructor that accepts String as a single argument
     */
    public static Object getStringWrapperObject(Map<String, String> properties, String key, Object defaultValue) {
        Object value = defaultValue;
        String className = properties.get(key + ".class");
        if (className != null) {
            String valueToString = properties.get(key + ".value");
            if (valueToString != null) {
                try {
                    Class<?> clazz = Class.forName(className);
                    value = clazz.getConstructor(String.class).newInstance(valueToString);
                } catch (InstantiationException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to create object from properties", e);
                }
            }
        }
        return value;
    }
}

