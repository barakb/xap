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

package org.openspaces.events.notify;

/**
 * If using a replicated space controls if the listener will be replicated between all the
 * replicated cluster members.
 *
 * @author kimchy (shay.banon)
 */
public enum ReplicateNotifyTemplateType {
    /**
     * The default value will be <code>false</code> if working against an embedded space instance,
     * and <code>true</code> otherwise.
     */
    DEFAULT,
    /**
     * Replicate notify templates between members.
     */
    TRUE,
    /**
     * Don't replicate notify templates between members.
     */
    FALSE
}
