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
 * Returns the JVM vendor short name e.g. Sun, IBM, BEA
 *
 * @author gershond
 * @version 6.5
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class OutputJVMVendorName {
    public static final String JVM_VENDOR;

    static {
        JVM_VENDOR = System.getProperty("java.vendor");
    }

    public static void main(String[] args) throws Exception {
        try {
            if (JVM_VENDOR != null) {
                String shortJvmVendorName = JVM_VENDOR.substring(0, JVM_VENDOR.indexOf(' '));
                System.out.println(shortJvmVendorName);
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println("Could not parse java.vendor property: " + JVM_VENDOR);
        }
        System.exit(1);
    }
}