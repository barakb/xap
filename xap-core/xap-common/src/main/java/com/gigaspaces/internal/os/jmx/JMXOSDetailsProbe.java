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

package com.gigaspaces.internal.os.jmx;

import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSDetails.OSNetInterfaceDetails;
import com.gigaspaces.internal.os.OSDetailsProbe;
import com.gigaspaces.start.SystemInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JMXOSDetailsProbe implements OSDetailsProbe {

    private static final String uid = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();
    private static final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    private static Method getTotalSwapSpaceSize;
    private static Method getTotalPhysicalMemorySize;

    static {

        try {
            Class sunOperatingSystemMXBeanClass = JMXOSDetailsProbe.class.getClassLoader().loadClass("com.sun.management.OperatingSystemMXBean");
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getTotalSwapSpaceSize");
                method.setAccessible(true);
                getTotalSwapSpaceSize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getTotalPhysicalMemorySize");
                method.setAccessible(true);
                getTotalPhysicalMemorySize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
        } catch (ClassNotFoundException e) {
            // not using sun. can't get the information
        }
    }

    public OSDetails probeDetails() {
        if (uid == null) {
            return new OSDetails();
        }
        long totalSwapSpaceSize = -1;
        if (getTotalSwapSpaceSize != null) {
            try {
                totalSwapSpaceSize = (Long) getTotalSwapSpaceSize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }
        long totalPhysicalMemorySize = -1;
        if (getTotalPhysicalMemorySize != null) {
            try {
                totalPhysicalMemorySize = (Long) getTotalPhysicalMemorySize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }

        OSNetInterfaceDetails[] osNetInterfaceDetailsArray = retrieveOSNetInterfaceDetails();

        return new OSDetails(uid, operatingSystemMXBean.getName(), operatingSystemMXBean.getArch(),
                operatingSystemMXBean.getVersion(), operatingSystemMXBean.getAvailableProcessors(),
                totalSwapSpaceSize, totalPhysicalMemorySize,
                localHostName, localHostAddress, osNetInterfaceDetailsArray, null, null);
    }

    private OSNetInterfaceDetails[] retrieveOSNetInterfaceDetails() {
        OSNetInterfaceDetails[] osNetInterfaceDetailsArray = null;

        try {
            Class networkInterfaceClass = Class.forName("java.net.NetworkInterface");
            Method method = networkInterfaceClass.getMethod("getHardwareAddress");

            Enumeration<NetworkInterface> networkInterfacesEnum =
                    NetworkInterface.getNetworkInterfaces();
            List<OSNetInterfaceDetails> interfacesList =
                    new ArrayList<OSNetInterfaceDetails>();

            while (networkInterfacesEnum.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfacesEnum.nextElement();

                byte[] hardwareAddress = (byte[]) method.invoke(networkInterface);

                if (hardwareAddress != null) {
                    String hardwareAddressStr = "";


                    for (int index = 0; index < hardwareAddress.length; index++) {
                        int val = hardwareAddress[index];
                        if (val < 0) {
                            val = 256 + val;
                        }

                        String hexStr = Integer.toString(val, 16).toUpperCase();
                        //add zero if this is only one char
                        if (hexStr.length() == 1) {
                            hexStr = 0 + hexStr;
                        }

                        hardwareAddressStr += hexStr;
                        if (index < hardwareAddress.length - 1) {
                            hardwareAddressStr += ":";
                        }
                    }

                    String name = networkInterface.getName();
                    String displayName = networkInterface.getDisplayName();

                    OSNetInterfaceDetails osNetInterfaceDetails =
                            new OSNetInterfaceDetails(hardwareAddressStr, name, displayName);

                    //add network interface details to list
                    interfacesList.add(osNetInterfaceDetails);
                }
            }

            osNetInterfaceDetailsArray =
                    interfacesList.toArray(new OSNetInterfaceDetails[0]);
        } catch (ClassNotFoundException ce) {

        } catch (NoSuchMethodException me) {

        } catch (IllegalAccessException ae) {

        } catch (InvocationTargetException te) {

        } catch (SocketException se) {

        }

        return osNetInterfaceDetailsArray;
    }
}