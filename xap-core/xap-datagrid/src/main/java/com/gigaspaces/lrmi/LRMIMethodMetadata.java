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

package com.gigaspaces.lrmi;

/**
 * Holds LRMI method metadata.
 *
 * @author idan
 * @since 8.0.3
 */
@com.gigaspaces.api.InternalApi
public class LRMIMethodMetadata {

    private final Boolean oneWay;
    private final Boolean async;

    public LRMIMethodMetadata(Boolean oneWay) {
        this(oneWay, null);
    }

    public LRMIMethodMetadata(Boolean oneWay, Boolean async) {
        this.oneWay = oneWay;
        this.async = async;
    }

    /**
     * @return whether the method should be invoked in a one-way manner.
     */
    public boolean isOneWay() {
        return oneWay;
    }

    public boolean isOneWayPresence() {
        return oneWay != null;
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isAsyncPresence() {
        return async != null;
    }
}
