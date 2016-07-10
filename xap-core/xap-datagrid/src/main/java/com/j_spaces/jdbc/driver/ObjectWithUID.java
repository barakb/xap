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


package com.j_spaces.jdbc.driver;

import java.io.Serializable;

/**
 * @author Michael Mitrani
 *
 *         This object is the parent of clasess that hold space entries uids, like the Clob and Blob
 *         implementations
 */
@com.gigaspaces.api.InternalApi
public class ObjectWithUID implements Serializable {
    private static final long serialVersionUID = -9005301568221991647L;

    protected String entryUID;
    protected int objectIndex; //this object original index in the ExternalEntry
    //it was taken from.

    public ObjectWithUID() {/*empty*/}

    /**
     * @return Returns the entryUID.
     */
    public String getEntryUID() {
        return entryUID;
    }

    /**
     * @param entryUID The entryUID to set.
     */
    public void setEntryUID(String entryUID) {
        this.entryUID = entryUID;
    }

    /**
     * @return Returns the objectIndex.
     */
    public int getObjectIndex() {
        return objectIndex;
    }

    /**
     * @param objectIndex The objectIndex to set.
     */
    public void setObjectIndex(int objectIndex) {
        this.objectIndex = objectIndex;
    }
}
