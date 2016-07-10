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

package com.gigaspaces.internal.os.sigar;

import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSDetails.OSDriveDetails;
import com.gigaspaces.internal.os.OSDetails.OSNetInterfaceDetails;
import com.gigaspaces.internal.os.OSDetails.OSVendorDetails;
import com.gigaspaces.internal.os.OSDetailsProbe;
import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.start.SystemInfo;

import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SigarOSDetailsProbe implements OSDetailsProbe {

    private static final String uid = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();

    private final Sigar sigar;

    public SigarOSDetailsProbe() {
        sigar = SigarHolder.getSigar();
    }

    public OSDetails probeDetails() throws Exception {
        OperatingSystem sys = OperatingSystem.getInstance();
        org.hyperic.sigar.CpuInfo[] infos = sigar.getCpuInfoList();

        String[] netInterfaceList = sigar.getNetInterfaceList();
        OSNetInterfaceDetails[] netInterfaceConfigArray = new
                OSNetInterfaceDetails[netInterfaceList.length];

        for (int index = 0; index < netInterfaceList.length; index++) {
            String interfaceName = netInterfaceList[index];
            NetInterfaceConfig config = sigar.getNetInterfaceConfig(interfaceName);
            String addr = config.getHwaddr();
            String name = config.getName();
            String description = config.getDescription();
            if (description == null) {
                //can happen on Windows when the network interface has no description
                description = "";
            }
            OSNetInterfaceDetails netInterfaceConfig =
                    new OSNetInterfaceDetails(addr, name, description);
            netInterfaceConfigArray[index] = netInterfaceConfig;
        }

        List<OSDriveDetails> drives = new ArrayList<OSDriveDetails>();

        for (FileSystem drive : sigar.getFileSystemList()) {
            if (drive.getType() == FileSystem.TYPE_LOCAL_DISK ||
                    drive.getType() == FileSystem.TYPE_NETWORK) {

                String dirName = drive.getDirName();
                FileSystemUsage fileSystemUsage = sigar.getFileSystemUsage(dirName);
                long capacityInKB = fileSystemUsage.getTotal(); // convert KB to MB
                drives.add(new OSDriveDetails(dirName, capacityInKB));
            }
        }

        return new OSDetails(uid, sys.getName(), sys.getArch(), sys.getVersion(), infos[0].getTotalCores(),
                sigar.getSwap().getTotal(), sigar.getMem().getTotal(),
                localHostName, localHostAddress, netInterfaceConfigArray,
                drives.toArray(new OSDriveDetails[drives.size()]),
                new OSVendorDetails(sys.getVendor(), sys.getVendorCodeName(), sys.getVendorName(), sys.getVendorVersion()));
    }
}
