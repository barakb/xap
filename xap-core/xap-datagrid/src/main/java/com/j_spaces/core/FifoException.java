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
 * @(#)FifoException.java   Dec 3, 2006
 *
 * Copyright 2006 GigaSpaces Technologies Inc.
 */

package com.j_spaces.core;

/**
 * Title:        The J-Spaces Platform Description: Copyright:    Copyright (c) J-Spaces Team
 * Company:      J-Spaces Technologies
 *
 * @author J-Spaces Team
 * @version 1.0
 */

/**
 * this exception is thrown when a searching entry or transaction is rejected in time of fifo
 * initial search
 */


@com.gigaspaces.api.InternalApi
public class FifoException extends Exception {
    private static final long serialVersionUID = 7716065918777101418L;

    public FifoException() {
    }


    public String toString() {
        return super.toString();
    }

    /**
     * Override the method to avoid expensive stack build and synchronization, since no one uses it
     * anyway.
     */
    @Override
    public Throwable fillInStackTrace() {
        // TODO Auto-generated method stub
        return null;
    }

}
