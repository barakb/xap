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

/*
 * @(#)RuntimeInfo.java 1.0  02.11.2004  22:33:20
 */

package com.gigaspaces.admin.cli;

import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.start.SystemInfo;

import java.io.File;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Show the System/GigaSpaces information. Calling the show() static method will be print to
 * System.out Example: System Environment : System: OS Version: 10.4.4 Architecture: ppc OS Name:
 * Mac OS X Number Of Processors: 1 J2SE Support: VM Vendor: "Apple Computer, Inc." Using Java Home:
 * /System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home Java(TM) 2 Runtime Environment,
 * Standard Edition Java HotSpot(TM) Client VM (build 1.5.0_05-48 ) JVM Memory: Max Heap Size (KB):
 * 64832 Current Allocated Heap Size (KB): 1584 Network Interfaces Information: Host Name:
 * [pc-pantera] Network Interface Name: en0 / en0 IP Address: 192.138.12.224 IP Address:
 * fe80:0:0:0:20d:93ff:fe32:6828%4 Network Interface Name: lo0 / lo0 IP Address: 127.0.0.1 IP
 * Address: fe80:0:0:0:0:0:0:1%1 IP Address: 0:0:0:0:0:0:0:1%0 GigaSpaces Platform: Version: 5.0EE
 * Build: 1359
 *
 * - JVM System properties - Network Card interface info - OS System variables - File content of
 * GS/config directory: *.xml, *.properties and *.config only.
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/
@com.gigaspaces.api.InternalApi
public class RuntimeInfo {

    private static boolean isFirstTime = true;

    public static String getEnvironmentInfoIfFirstTime() {
        return isFirstTime ? getEnvironmentInfo() : "";
    }

    public static String getEnvironmentInfo() {
        isFirstTime = false;
        return XapExtensions.getInstance().getXapRuntimeReporter().generate("System Environment", '*');
    }

    /**
     * Returns full environment info: - JVM System properties - Network Card interface info - OS
     * System variables - File content of GS/config directory: *.xml, *.properties and config only
     **/
    public static String getEnvironmentInfo(boolean isFullInfo) {
        String shortEnvInfo = getEnvironmentInfo();
        if (!isFullInfo)
            return shortEnvInfo;

        String gsContentDir = getGSConfigContent();
        String systemEnv = getSystemEnv();
        String javaSysProp = getJavaSystemProperties();

        StringBuilder gsDumpBuf = new StringBuilder("\n\n =========== START GIGASPACES ENVIRONMENT REPORT ");
        gsDumpBuf.append(new Date()).append(" ==========");

        gsDumpBuf.append(shortEnvInfo);
        gsDumpBuf.append("\n\n").append(gsContentDir);

        if (systemEnv != null)
            gsDumpBuf.append("\n\n").append(systemEnv);

        gsDumpBuf.append("\n\n" + javaSysProp);
        gsDumpBuf.append("\n\n ===========  END GIGASPACES ENVIRONMENT REPORT =========== ");

        return gsDumpBuf.toString();
    }

    /**
     * returns Java System Properties
     */
    private static String getJavaSystemProperties() {
        StringBuilder sb = new StringBuilder("JVM System properties: ");
        Properties prop = System.getProperties();
        Enumeration<String> iter = (Enumeration<String>) prop.propertyNames();
        while (iter.hasMoreElements()) {
            String key = iter.nextElement();
            sb.append("\n\t\t" + key + "=" + prop.getProperty(key));
        }

        SecurityManager sm = System.getSecurityManager();
        if (sm != null)
            sb.append("\n\t\t Security Manager: " + sm);

        return sb.toString();
    }

    /**
     * Returns Operation System Enviroment
     */
    private static String getSystemEnv() {
        Map<String, String> variables = null;
        try {
            variables = System.getenv();
        } catch (Throwable ex) {
            /** if we got here, current jdk doesn't support System.getenv() method. */
            return null;
        }

        String osName = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        StringBuilder sb = new StringBuilder(osName).append(' ').append(osVersion).append(" System Environment:");
        Set<Map.Entry<String, String>> entries = variables.entrySet();

        for (Map.Entry<String, String> entry : entries) {
            sb.append("\n\t\t").append(entry.getKey()).append('=').append(entry.getValue());
        }

        return sb.toString();
    }

    /**
     * @return GS config directory content only for *.xml,  *.properties and *.config files.
     **/
    private static String getGSConfigContent() {
        boolean isWarning = false;
        StringBuilder sb = new StringBuilder();
        File homeDirFile = new File(SystemInfo.singleton().getXapHome());

        File configDir = new File(homeDirFile.getPath() + File.separator + "config");
        if (configDir.isDirectory() && configDir.canRead()) {
            isWarning = false;
            sb.append("GigaSpaces Config directory tree structure: ");
            sb.append(configDir.getAbsolutePath() + " : ");

            buildFileDirectoryContent(1, configDir.getPath(), sb);
        }

        if (isWarning)
            sb.append("WARNING: Config directory under [" + homeDirFile.getAbsolutePath() + "] not found.");

        return sb.toString();
    }

    /**
     * Builds recursively the full directory content: dir-name, file-name.
     **/
    private static void buildFileDirectoryContent(int leafCount, String sourceDir, StringBuilder destStrBuffer) {
        File dir = new File(sourceDir);
        File[] files = BootIOUtils.listFiles(dir);

        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                String fileName = files[i].getName();

                if (fileName.endsWith(".properties") || fileName.endsWith(".xml") || fileName.endsWith(".config")) {
                    destStrBuffer.append("\n\t");
                    for (int z = 0; z < leafCount; z++)
                        destStrBuffer.append("\t");

                    destStrBuffer.append("- " + fileName);
                }
            } else {
                leafCount++;
                destStrBuffer.append("\n");
                for (int z = 0; z < leafCount; z++)
                    destStrBuffer.append("\t");

                destStrBuffer.append("-/" + files[i].getName());

                /** build sub-directory content */
                buildFileDirectoryContent(leafCount, files[i].getPath(), destStrBuffer);
                leafCount--;
            }
        }
    }

    public static void main(String[] args) {
        /** print out to the default output stream full GS environment */
        System.out.println(RuntimeInfo.getEnvironmentInfo(true));
    }
}