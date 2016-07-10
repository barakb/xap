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


package org.openspaces.events;

/**
 * A template provider allowing for custom implementations to provide a template used for matching
 * on events to receive.
 *
 * <p> Can be used when a {@link org.openspaces.events.SpaceDataEventListener
 * SpaceDataEventListener} is provided that also controls the template associated with it. This of
 * course depends on the container and containers should take it into account (such as {@link
 * org.openspaces.events.polling.SimplePollingEventListenerContainer} and {@link
 * org.openspaces.events.notify.SimpleNotifyEventListenerContainer}.
 *
 * @author kimchy
 */
public interface EventTemplateProvider {

    /**
     * Returns the template that will be used for matching on events.
     */
    Object getTemplate();
}
