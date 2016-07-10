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

/*
 * @(#)LRMINotifyDelegatorListener 1.0   23/05/2003  9:50AM
 */

package com.gigaspaces.internal.lrmi.stubs;

import com.j_spaces.core.client.INotifyDelegator;

import net.jini.core.event.RemoteEventListener;

/**
 * NotifyDelegator stub to get notification via LRMI layer.
 *
 * @author Igor Goldenberg
 * @version 2.5
 **/
public class LRMINotifyDelegatorListener extends LRMIRemoteEventListener implements INotifyDelegator {
    private static final long serialVersionUID = 1L;

    // NOTE, here just for externalizable
    public LRMINotifyDelegatorListener() {
    }

    /**
     * constructor to initialize NotifyDelegator remoteEventListener
     **/
    public LRMINotifyDelegatorListener(RemoteEventListener directObjRef, RemoteEventListener dynamicProxy) {
        super(directObjRef, dynamicProxy);
    }
}