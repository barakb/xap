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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.version.PlatformLogicalVersion;

/**
 * Thrown when attempting to invoke a remote method using LRMI on a service which does not support
 * this method
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ServiceUnsupportedOperationException
        extends RuntimeException {

    /** */
    private static final long serialVersionUID = 1L;

    private final String localOfficialVersion;
    private final String serviceOfficialVersion;
    private final PlatformLogicalVersion localPlatformlogicalVersion;
    private final PlatformLogicalVersion serverPlatformLogicalVersion;

    public ServiceUnsupportedOperationException(String msg,
                                                String localOfficialVersion, String remoteOfficialVersion,
                                                PlatformLogicalVersion localPlatformlogicalVersion,
                                                PlatformLogicalVersion serverPlatformLogicalVersion) {
        super(msg);
        this.localOfficialVersion = localOfficialVersion;
        this.serviceOfficialVersion = remoteOfficialVersion;
        this.localPlatformlogicalVersion = localPlatformlogicalVersion;
        this.serverPlatformLogicalVersion = serverPlatformLogicalVersion;
    }

    /**
     * @return local official version
     */
    public String getLocalOfficialVersion() {
        return localOfficialVersion;
    }

    /**
     * @return service official version
     */
    public String getServiceOfficialVersion() {
        return serviceOfficialVersion;
    }

    /**
     * @return local logical version
     */
    public PlatformLogicalVersion getLocalPlatformlogicalVersion() {
        return localPlatformlogicalVersion;
    }

    /**
     * @return service logical version
     */
    public PlatformLogicalVersion getServerPlatformLogicalVersion() {
        return serverPlatformLogicalVersion;
    }

}
