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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

/**
 * Defines a set of constants that gives an information on the state of a JavaSpace.
 *
 * @author Michael Konnikov
 * @version 1.0
 * @see com.j_spaces.core.admin.IRemoteJSpaceAdmin#getState()
 * @see com.sun.jini.start.LifeCycle
 * @since 4.0
 */
public interface ISpaceState {
    /**
     * Service is starting.
     */
    public static final int STARTING = 0;

    /**
     * Service is started.
     */
    public static final int STARTED = 1;

    /**
     * Service is stopped.
     */
    public static final int STOPPED = 2;


    /**
     * Service has been aborted.
     */
    public static final int ABORTED = 3;

    /**
     * Service is unreachable.
     */
    public static final int UNKNOWN = 4;

    /**
     * Service is during abort process
     *
     * @since 7.1.1 Should not be published on the lus as is due to backward
     */
    public static final int ABORTING = 5;

}