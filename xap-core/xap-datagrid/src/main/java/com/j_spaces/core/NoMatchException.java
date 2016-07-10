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

import com.gigaspaces.internal.server.storage.IEntryHolder;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

/**
 * this exception is thrown when after update or RB of update the entry and template no more match
 */
@com.gigaspaces.api.InternalApi
public class NoMatchException extends Exception {
    private static final long serialVersionUID = -67607005399884967L;

    final private IEntryHolder _entry;

    public NoMatchException(IEntryHolder entry) {
        _entry = entry;
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

    @Override
    public String toString() {
        return "No Match Exception: " + _entry;
    }
}