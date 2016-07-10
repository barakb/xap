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

package org.openspaces.core.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Barak Bar Orion on 3/20/16. Used compute a new Properties with some values translated
 * to "***", to be used for toString() method to hide sensitive data from logs.
 *
 * @since 11.0
 */
@SuppressWarnings("WeakerAccess")
public class SanitizeUtil {

    public static Properties sanitize(Properties properties, String... keywords) {
        if (properties == null) {
            return null;
        }
        Properties res = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if (contains(key, keywords)) {
                res.setProperty(key, "***");
            } else {
                res.setProperty(key, properties.getProperty(key));
            }
        }
        return res;
    }

    public static Map<String, Properties> sanitize(Map<String, Properties> map, String... keywords) {
        Map<String, Properties> res = new HashMap<String, Properties>();
        for (String key : map.keySet()) {
            res.put(key, sanitize(map.get(key), keywords));
        }
        return res;
    }

    private static boolean contains(String key, String[] keywords) {
        for (String keyword : keywords) {
            if (key.toLowerCase().contains(keyword.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}
