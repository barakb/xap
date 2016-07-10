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

package com.j_spaces.core;

import com.gigaspaces.cluster.replication.IReplicationPacket;

/**
 * An UnknownTypeException is thrown when a space receives an entry/template of type that is not in
 * his type table, and the EntryPacket does not contain full type information. The client should
 * catch this exception and recall the method with full information in the EntryPacket.
 *
 * @see com.gigaspaces.internal.transport.IEntryPacket
 */
@com.gigaspaces.api.InternalApi
public class UnknownTypeException
        extends Exception {
    private static final long serialVersionUID = -6396162967502290947L;

    final private String className;

    /**
     * the event which caused to UnknowTypeException
     */
    private IReplicationPacket theCausedEvent;

    public UnknownTypeException(String msg, String className) {
        super(msg);

        this.className = className;
    }

    public String getUnknownClassName() {
        return className;
    }

    /**
     * returns event object which caused to UnknowTypeException
     */
    public IReplicationPacket getCauseEvent() {
        return theCausedEvent;
    }

    /**
     * set the event(Entry, IReplicationPacket etc...) object which caused to UnknowTypeException
     * (optionally)
     */
    public void setCauseEvent(IReplicationPacket theCausedEvent) {
        this.theCausedEvent = theCausedEvent;
    }
}