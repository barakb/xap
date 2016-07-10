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


package com.j_spaces.core.client;

import com.j_spaces.core.DetailedUnusableEntryException;

/**
 * This exception is thrown when update/take operation is rejected as a result of optimistic locking
 * version conflict.
 *
 * @author Yechiel
 * @version 3.0
 **/

public class EntryVersionConflictException extends DetailedUnusableEntryException {
    private static final long serialVersionUID = -3939371479314993420L;

    private String UID;
    private int spaceVersionID;
    private int clientVersionID;
    private String operation;

    public EntryVersionConflictException(String UID, int spaceVersionID, int clientVersionID, String operation) {
        super("Entry Version ID conflict, Operation rejected. Operation=" + operation + " UID=" + UID +
                " space entry version=" + spaceVersionID + " client version=" + clientVersionID);

        this.UID = UID;
        this.spaceVersionID = spaceVersionID;
        this.clientVersionID = clientVersionID;
        this.operation = operation;
    }

    /**
     * Return entry UID
     **/
    public String getUID() {
        return UID;
    }

    /**
     * Return entry Space Version ID
     **/
    public int getSpaceVersionID() {
        return spaceVersionID;
    }

    /**
     * Return entry client Version ID
     **/
    public int getClientVersionID() {
        return clientVersionID;
    }

    /**
     * Return the space operation caused the conflict Take or Update
     **/
    public String getOperation() {
        return operation;
    }
}