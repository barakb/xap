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

package com.gigaspaces.grid.zone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class ZoneHelper {

    private static final String ZONE_PROPERTY = "com.gs.zones";

    public static String[] getSystemZones() {
        String zones = System.getProperty(ZONE_PROPERTY);
        if (zones == null) {
            return null;
        }
        StringTokenizer st = new StringTokenizer(zones, ",");
        ArrayList<String> zonesList = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            String zone = st.nextToken();
            if (zone != null && zone.length() > 0) {
                zonesList.add(zone);
            }
        }
        return zonesList.toArray(new String[zonesList.size()]);
    }

    public static boolean zoneExists(String zoneToFind, String[] zones) {
        if (zones == null) {
            return false;
        }
        for (String zone : zones) {
            if (zone.equals(zoneToFind)) {
                return true;
            }
        }
        return false;
    }

    public static String unparse(Map<String, Integer> maxInstancesPerZone) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : maxInstancesPerZone.entrySet()) {
            sb.append(entry.getKey()).append("/").append(entry.getValue()).append(',');
        }
        return sb.toString();
    }

    public static Map<String, Integer> parse(String maxInstancesPerZone) {
        if (maxInstancesPerZone == null) {
            return null;
        }
        Map<String, Integer> result = new HashMap<String, Integer>();
        StringTokenizer st = new StringTokenizer(maxInstancesPerZone, ",");
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (token != null && token.length() > 0) {
                int index = token.indexOf('/');
                if (index == -1) {
                    result.put(token, 1);
                } else {
                    result.put(token.substring(0, index), Integer.parseInt(token.substring(index + 1)));
                }
            }
        }
        return result;
    }

    public static LinkedHashSet<String> parseZones(String zone) {
        LinkedHashSet<String> zones = new LinkedHashSet<String>();
        if (zone != null) {
            String[] zonesArr = zone.split(",");
            for (String z : zonesArr) {
                zones.add(z.trim());
            }
        }
        return zones;
    }
}
