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

package com.gigaspaces.internal.version;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.ErrorManager;

@com.gigaspaces.api.InternalApi
public class PlatformVersion {
    private static final PlatformVersion instance = new PlatformVersion();

    private final String version;
    private final String milestone;
    private final String buildNumber;
    private final String shortOfficialVersion;
    private final String officialVersion;
    private final byte majorVersion;
    private final byte minorVersion;
    private final byte spVersion;
    private final int shortBuildNumber;
    private final int subBuildNumber;
    private final Map<String, String> revisions;
    private final String productHelpUrl;

    private PlatformVersion() {
        Properties properties = getVersionPropertiesFromFile("com/gigaspaces/internal/version/PlatformVersion.properties");
        version = properties.getProperty("xap.version", "12.0.0");
        milestone = properties.getProperty("xap.milestone", "m1");
        buildNumber = properties.getProperty("xap.build.number", "15000");
        revisions = loadRevisions(properties);

        shortOfficialVersion = "XAP " + version + " " + milestone.toUpperCase();
        officialVersion = "GigaSpaces " + shortOfficialVersion + " (build " + buildNumber + formatRevisions() + ")";

        String[] versionTokens = version.split("\\.");
        majorVersion = Byte.parseByte(versionTokens[0]);
        minorVersion = Byte.parseByte(versionTokens[1]);
        spVersion = Byte.parseByte(versionTokens[2]);

        final String[] buildNumberTokens = buildNumber.split("-");
        shortBuildNumber = Integer.parseInt(buildNumberTokens[0]);
        subBuildNumber = buildNumberTokens.length == 1 ? 0 : Integer.parseInt(buildNumberTokens[1]);

        productHelpUrl = "http://docs.gigaspaces.com/xap" + String.valueOf(majorVersion) + String.valueOf(minorVersion);
    }

    private static Map<String, String> loadRevisions(Properties properties) {
        final String REVISION_PREFIX = "xap.git.sha";
        // Using treemap to gain alphabetical sort automatically.
        final Map<String, String> revisions = new TreeMap<String, String>();
        for (String property : properties.stringPropertyNames()) {
            if (property.startsWith(REVISION_PREFIX)) {
                String name = property.equals(REVISION_PREFIX) ? "xap" : "xap-" + property.substring(REVISION_PREFIX.length() + 1);
                revisions.put(name, properties.getProperty(property));
            }
        }
        return revisions;
    }

    private String formatRevisions() {
        if (revisions.isEmpty())
            return "";
        if (revisions.size() == 1)
            return ", revision " + revisions.values().iterator().next();

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : revisions.entrySet()) {
            if (sb.length() != 0)
                sb.append(' ');
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return ", revisions: " + sb.toString();
    }

    public static PlatformVersion getInstance() {
        return instance;
    }

    public static void main(String args[]) {
        System.out.println(PlatformVersion.getOfficialVersion());
    }

    private static Properties getVersionPropertiesFromFile(String path) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = PlatformVersion.class.getClassLoader().getResourceAsStream(path);
            if (inputStream != null) {
                properties.load(inputStream);
                inputStream.close();
            }
        } catch (Throwable t) {
            ErrorManager errorManager = new ErrorManager();
            errorManager.error("Failed to load version properties from " + path, new Exception(t), ErrorManager.OPEN_FAILURE);
        }
        return properties;
    }

    public byte getMajorVersion() {
        return majorVersion;
    }

    public byte getMinorVersion() {
        return minorVersion;
    }

    public byte getServicePackVersion() {
        return spVersion;
    }

    public int getShortBuildNumber() {
        return shortBuildNumber;
    }

    public int getSubBuildNumber() {
        return subBuildNumber;
    }

    /**
     * @return GigaSpaces XAP 8.0.6 GA (build 6191)
     */
    public static String getOfficialVersion() {
        return instance.officialVersion;
    }

    /**
     * @return XAP 8.0.6 GA
     */
    public static String getShortOfficialVersion() {
        return instance.shortOfficialVersion;
    }

    /**
     * @return e.g. 8.0.6
     */
    public static String getVersion() {
        return instance.version;
    }

    /**
     * @return e.g. 6191
     */
    public static String getBuildNumber() {
        return instance.buildNumber;
    }

    /**
     * @return e.g. ga
     */
    public static String getMilestone() {
        return instance.milestone;
    }

    public static String getProductHelpUrl() {
        return instance.productHelpUrl;
    }
}
