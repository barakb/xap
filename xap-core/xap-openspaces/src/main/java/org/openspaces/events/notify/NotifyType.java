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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Controls which type of notifications will be sent.
 *
 * @author kimchy
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NotifyType {

    /**
     * Should this listener be notified when write occurs and it matches the given template.
     */
    boolean write() default false;

    /**
     * Should this listener be notified when take occurs and it matches the given template.
     */
    boolean take() default false;

    /**
     * Should this listener be notified when update occurs and it matches the given template.
     */
    boolean update() default false;

    /**
     * Should this listener be notified when lease expiration occurs and it matches the given
     * template.
     */
    boolean leaseExpire() default false;

    /**
     * Should this listener be notified when entries that no longer match the provided template be
     * notified.
     */
    boolean unmatched() default false;

    /**
     * Should this listener be notified when entries that weren't match to a provided template
     * become match after an update occurs.
     *
     * @since 9.1
     */
    boolean matchedUpdate() default false;

    /**
     * Should this listener be notified when entries that were already match to a provided template
     * stays match after an update occurs.
     *
     * @since 9.1
     */
    boolean rematchedUpdate() default false;
}
