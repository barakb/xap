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

package com.gigaspaces.internal.io;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.prefs.Preferences;

/**
 * This class provides Windows Registry services.
 *
 * @author niv
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class WindowsRegistryUtils {
    // Windows Registry predefined keys:
    //private static final int HKEY_CLASSES_ROOT		= 0x80000000;
    private static final int HKEY_CURRENT_USER = 0x80000001;
    private static final int HKEY_LOCAL_MACHINE = 0x80000002;
    //private static final int HKEY_USERS 				= 0x80000003;
    //private static final int HKEY_PERFORMANCE_DATA	= 0x80000004;
    //private static final int HKEY_CURRENT_CONFIG		= 0x80000005;
    //private static final int HKEY_DYN_DATA			= 0x80000006;
    //private static final int HKEY_PERFORMANCE_TEXT	= 0x80000050;
    //private static final int HKEY_PERFORMANCE_NLSTEXT= 0x80000060;

    // Windows security masks:
    private static final int KEY_QUERY_VALUE = 0x0001;
    //    private static final int KEY_SET_VALUE		= 0x0002;
//    private static final int KEY_CREATE_SUB_KEY	= 0x0004;
    private static final int KEY_ENUMERATE_SUB_KEYS = 0x0008;
    private static final int KEY_NOTIFY = 0x0010;
    //	private static final int KEY_CREATE_LINK 		= 0x0020;
//    private static final int KEY_WOW64_64KEY 		= 0x0100;
//    private static final int KEY_WOW64_32KEY 		= 0x0200;
//    private static final int DELETE 				= 0x10000;
//    private static final int KEY_WRITE 			= 0x20000 + KEY_SET_VALUE + KEY_CREATE_SUB_KEY;
    private static final int KEY_READ = 0x20000 + KEY_QUERY_VALUE + KEY_ENUMERATE_SUB_KEYS + KEY_NOTIFY;
//    private static final int KEY_ALL_ACCESS 		= 0xf0000 + KEY_QUERY_VALUE + KEY_SET_VALUE + KEY_CREATE_SUB_KEY + KEY_ENUMERATE_SUB_KEYS + KEY_NOTIFY + KEY_CREATE_LINK;

    // Windows error codes:
    private static final int ERROR_SUCCESS = 0;
    private static final int ERROR_FILE_NOT_FOUND = 2;
    private static final int ERROR_ACCESS_DENIED = 5;

    // Constants used to interpret returns of native functions
    private static final int NATIVE_HANDLE = 0;
    private static final int ERROR_CODE = 1;
    //private static final int SUBKEYS_NUMBER = 0;
    //private static final int VALUES_NUMBER = 2;
    //private static final int MAX_KEY_LENGTH = 3;
    //private static final int MAX_VALUE_NAME_LENGTH = 4;
    //private static final int DISPOSITION = 2;
    //private static final int REG_CREATED_NEW_KEY = 1;
    //private static final int REG_OPENED_EXISTING_KEY = 2;
    //private static final int NULL_NATIVE_HANDLE = 0;

    private static Preferences _prefsInstance;

    private static final Method _openKeyMethod;
    private static final Method _closeKeyMethod;
    private static final Method _queryValueMethod;

    static {
        _prefsInstance = Preferences.userRoot();
        Class<?> prefsClass = _prefsInstance.getClass();

        try {
            _openKeyMethod = prefsClass.getDeclaredMethod("WindowsRegOpenKey", int.class, byte[].class, int.class);
            _openKeyMethod.setAccessible(true);
            _closeKeyMethod = prefsClass.getDeclaredMethod("WindowsRegCloseKey", int.class);
            _closeKeyMethod.setAccessible(true);
            _queryValueMethod = prefsClass.getDeclaredMethod("WindowsRegQueryValueEx", int.class, byte[].class);
            _queryValueMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new WindowsRegistryException("Error initializing Windows Registry utils.", e);
        }
    }

    /**
     * Opens a registry key.
     *
     * @param baseKey One of the predefined keys, or a key previously returned by openKey.
     * @param keyPath Path relative to baseKey.
     * @return Handle to opened key. Null if not exists.
     */
    private static Integer openKey(Integer baseKey, String keyPath) {
        return openKey(baseKey, keyPath, KEY_READ);
    }

    /**
     * Opens a registry key.
     *
     * @param baseKey       One of the predefined keys, or a key previously returned by openKey.
     * @param keyPath       Path relative to baseKey.
     * @param securityToken Security modifiers for key.
     * @return Handle to opened key. Null if not exists.
     */
    private static Integer openKey(Integer baseKey, String keyPath, int securityToken) {
        // Invoke registry operation, get result:
        int[] result = invokeRegistryMethod(_openKeyMethod, baseKey, toByteArray(keyPath), securityToken);

        // Check result:
        int errorCode = result[ERROR_CODE];
        switch (errorCode) {
            case ERROR_SUCCESS:
                return result[NATIVE_HANDLE];
            case ERROR_FILE_NOT_FOUND:
                return null;
            case ERROR_ACCESS_DENIED:
                throw new WindowsRegistryException("Access denied. Path=" + keyPath + ", securityToken=" + securityToken);
            default:
                throw new WindowsRegistryException("Error. Code=" + errorCode + ", Path=" + keyPath + ", securityToken=" + securityToken);
        }
    }

    /**
     * Closes a previously opened key.
     *
     * @param keyHandle handle to opened key.
     */
    private static void closeKey(Integer keyHandle) {
        if (keyHandle == null)
            return;

        // Invoke registry operation, get result:
        Integer result = invokeRegistryMethod(_closeKeyMethod, keyHandle);
        if (result != ERROR_SUCCESS)
            throw new WindowsRegistryException("Error. Code=" + result);
    }

    /**
     * Queries the specified key handle for the specified value.
     *
     * @param keyHandle handle to key to query.
     * @param valueName name of value to query.
     * @return The specified value's data. If not exists, null.
     */
    private static String queryKeyValue(Integer keyHandle, String valueName) {
        // Translate String arguments to raw bytes:
        byte[] rawValueName = toByteArray(valueName);
        // Invoke registry operations:
        byte[] rawResult = invokeRegistryMethod(_queryValueMethod, keyHandle, rawValueName);
        // Translate result to java string:
        if (rawResult == null)
            return null;
        // Translate the byte array to string (remove the null-termination):
        String result = new String(rawResult, 0, rawResult.length - 1);
        return result;
    }

    /**
     * Opens the specified key path and reads the specified value.
     */
    private static String readKeyValue(Integer keyHandle, String keyPath, String valueName) {
        Integer actualKeyHandle = null;

        try {
            // Open key:
            actualKeyHandle = openKey(keyHandle, keyPath);
            // If key does not exists, abort:
            if (actualKeyHandle == null)
                return null;
            // Read value:
            return queryKeyValue(actualKeyHandle, valueName);
        } finally {
            // Close key, if open:
            closeKey(actualKeyHandle);
        }
    }

    /**
     * Opens the specified key path and reads the specified value from the current user settings.
     *
     * @param valueName name of value
     */
    public static String readKeyValueCurrentUser(String keyPath, String valueName) {
        return readKeyValue(HKEY_CURRENT_USER, keyPath, valueName);
    }

    /**
     * Opens the specified key path and reads the specified value from the local machine settings.
     */
    public static String readKeyValueLocalMachine(String keyPath, String valueName) {
        return readKeyValue(HKEY_LOCAL_MACHINE, keyPath, valueName);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeRegistryMethod(Method method, Object... args) {
        try {
            return (T) method.invoke(_prefsInstance, args);
        } catch (InvocationTargetException e) {
            throw new WindowsRegistryException("Error executing registry method " + method.getName(), e);
        } catch (IllegalAccessException e) {
            throw new WindowsRegistryException("Error executing registry method " + method.getName(), e);
        }
    }

    /**
     * Returns a null-terminated byte representation of the string.
     */
    private static byte[] toByteArray(String str) {
        int length = str.length();
        byte[] result = new byte[length + 1];
        for (int i = 0; i < length; i++)
            result[i] = (byte) str.charAt(i);
        result[length] = 0;
        return result;
    }
}
