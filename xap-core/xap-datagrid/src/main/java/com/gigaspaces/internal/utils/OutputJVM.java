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

package com.gigaspaces.internal.utils;


/**
 * Returns the JVM version short prefixed number e.g. 1.5 1.6
 *
 * @author kobi
 * @version 7.1
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class OutputJVM {
    public static final String JVM_VERSION;
    public static final String JVM_VENDOR;

    private static final String JVM_ARCH;

    static {
        JVM_VERSION = System.getProperty("java.version");
        JVM_VENDOR = System.getProperty("java.vendor");
        JVM_ARCH = System.getProperty("sun.arch.data.model");

    }

    public static void main(String[] args) throws Exception {
        try {
            if (JVM_VERSION != null && JVM_VENDOR != null && JVM_ARCH != null) {
                String shortJvmVendorName = JVM_VENDOR.substring(0, JVM_VENDOR.indexOf(' '));
                String shortJvmVersion = JVM_VERSION.substring(0, JVM_VERSION.lastIndexOf('.'));
                String shortJvmBuild = JVM_VERSION.substring(JVM_VERSION.indexOf('_') + 1, JVM_VERSION.length());

                System.out.println(shortJvmVendorName);
                System.out.println(shortJvmVersion);
                System.out.println(shortJvmBuild);
                System.out.println(JVM_ARCH);
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println("Could not parse java.version property: " + JVM_VERSION + " or " + JVM_VENDOR);
        }

        System.exit(1);
    }
}
