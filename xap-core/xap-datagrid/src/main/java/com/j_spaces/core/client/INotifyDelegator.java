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

import com.gigaspaces.events.ManagedRemoteEventListener;

/**
 * This is just a tag interface for the space to recognize the stub of the NotifyDelegator.
 *
 * This interface should be used when the entry is needed on notify.
 * <pre>
 * Example:
 * <code>
 * class MyNotifyDelegator implements INotifyDelegator
 * {
 *      public void notify(RemoteEvent event)
 *      {
 *          EntryArrivedRemoteEvent arrivedRemoteEvent = (EntryArrivedRemoteEvent)event;
 *          MyEntry myEntry = arrivedRemoteEvent.getEntry();
 *          ...
 *      }
 * }
 * </code>
 * </pre>
 *
 * @see NotifyDelegator
 * @deprecated
 */
@Deprecated
public interface INotifyDelegator extends ManagedRemoteEventListener {
}
