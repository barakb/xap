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

package com.gigaspaces.cluster.activeelection;

import java.rmi.RemoteException;

/**
 * This exception thrown when attempting to access an inactive space which runs as a backup of
 * primary space. As long as the the primary space is alive, clients cannot access the backup space
 * directly to perform space operation. When using clustered proxy, the operation will be
 * transparently routed to the active primary space. The {@link #getPrimarySpaceName()} method
 * return the primary space name the user should access. <p> Note: Prior to GigaSpaces 5.2
 * com.j_spaces.core.SpaceStandbyException was used to indicate client access to space running in
 * backup mode.
 *
 * @author Igor Goldenberg
 * @version 5.2
 * @see SpaceMode
 **/
public class InactiveSpaceException
        extends RemoteException {
    private static final long serialVersionUID = 1L;

    private final String primarySpaceMemberName;
    private final String inActiveSpaceMemberName;

    /**
     * Constructor.
     *
     * @param inActiveSpaceMemberName the backup-only space name.
     * @param primarySpaceMemberName  the alive primary space of this backup.
     */
    public InactiveSpaceException(String inActiveSpaceMemberName, String primarySpaceMemberName) {
        this(inActiveSpaceMemberName, primarySpaceMemberName, "[" + inActiveSpaceMemberName + "] servers as backup-only space and can't accept " +
                "any operation while primary [" + primarySpaceMemberName + "] space is alive.");
    }

    protected InactiveSpaceException(String inActiveSpaceMemberName, String primarySpaceMemberName, String msg) {
        super(msg);

        this.inActiveSpaceMemberName = inActiveSpaceMemberName;
        this.primarySpaceMemberName = primarySpaceMemberName;
    }


    /**
     * @return the primary space of this backup or <code>null</code> if this space undergoing
     * election process.
     */
    public String getPrimarySpaceName() {
        return primarySpaceMemberName;
    }

    /**
     * @return the inactive space name.
     */
    public String getInActiveSpaceName() {
        return inActiveSpaceMemberName;
    }
}