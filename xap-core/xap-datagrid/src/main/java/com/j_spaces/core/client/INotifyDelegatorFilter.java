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

import com.j_spaces.core.IJSpace;

import java.io.Serializable;

/**
 * This interface represents notify filter logic.
 *
 * @author Igor Goldenberg
 * @version 4.5
 **/
public interface INotifyDelegatorFilter
        extends Serializable {
    /**
     * Called inside of server on Notify Filter init.
     *
     * @param space          The space proxy which this filter belongs to, which may be used to
     *                       access the space. The reference to IJSpace proxy must be
     *                       <b><tt>transient</tt></b> inside the filter object.
     * @param notifyTemplate The notify template which this filter belong to.
     **/
    public void init(IJSpace space, Object notifyTemplate);

    /**
     * Notify the filter about an event.
     *
     * @param theEvent The event that occurred.
     * @return If <code>true</code> this event will be dispatched to the registered
     * MulticastNotifyDelegator.
     **/
    public boolean process(EntryArrivedRemoteEvent theEvent);


    /**
     * Called when notify template unregistered or space was shutdown.
     **/
    public void close();
}
