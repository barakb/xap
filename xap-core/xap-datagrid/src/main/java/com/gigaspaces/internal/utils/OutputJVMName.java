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
 * Returns the JVM name as short name e.g. HOTSPOT, JROCKIT, J9
 *
 * @author guyk
 * @since 8.0.1
 */
@com.gigaspaces.api.InternalApi
public class OutputJVMName {

    public static final String JVM_NAME;

    static {
        JVM_NAME = System.getProperty("java.vm.name");
    }

    public static void main(String[] args) throws Exception {
        try {
            if (JVM_NAME != null) {
                String[] split = JVM_NAME.split(" |\\(");
                String shortJvmName;
                if (split.length > 1)
                    shortJvmName = split[1];// e.g. IBM J9 VM --> J9 or Oracle HotSpot(R) --> HotSpot
                else
                    shortJvmName = split[0];
                System.out.println(shortJvmName.toUpperCase());
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println("Could not parse java.vm.name property: " + JVM_NAME);
        }
        System.exit(1);
    }
}