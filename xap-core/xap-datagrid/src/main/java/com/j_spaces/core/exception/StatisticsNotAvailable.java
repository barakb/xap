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


package com.j_spaces.core.exception;

import java.rmi.RemoteException;

/**
 * This exception is thrown in case when space statistics are not available.
 *
 * @author Michael Konnikov
 * @version 4.1
 */

public class StatisticsNotAvailable extends RemoteException {
    private static final long serialVersionUID = 1646784425146356902L;

    /**
     * Constructs a <code>SpaceStoppedException</code> with the specified detail message.
     *
     * @param s - the detail message
     */
    public StatisticsNotAvailable(String s) {
        super(s);
    }
}
