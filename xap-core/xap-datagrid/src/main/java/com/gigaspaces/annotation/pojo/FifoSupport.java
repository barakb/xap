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


package com.gigaspaces.annotation.pojo;

/**
 * Determines FIFO operations support.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public enum FifoSupport {
    /**
     * @deprecated Since 8.0.1 - Use DEFAULT instead.
     */
    @Deprecated
    NOT_SET,
    /**
     * FIFO support will be inherited from the superclass. If superclass doesn't have FIFO settings,
     * FifoSupport will be set to OFF.
     *
     * @since 8.0.1
     */
    DEFAULT,
    /**
     * FIFO operations are not supported for this class.
     */
    OFF,
    /**
     * FIFO operations are supported for this class. Use {@link com.j_spaces.core.client.ReadModifiers#FIFO}
     * to perform a FIFO operation.
     */
    OPERATION,
    /**
     * FIFO operations are supported for this class. All operations involving this class are FIFO.
     */
    ALL;
}
