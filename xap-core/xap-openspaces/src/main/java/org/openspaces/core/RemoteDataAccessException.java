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


package org.openspaces.core;

import org.springframework.dao.DataAccessException;

import java.rmi.RemoteException;

/**
 * Wraps {@link java.rmi.RemoteException} that is thrown from the Space.
 *
 * @author kimchy
 */
public class RemoteDataAccessException extends DataAccessException {

    private static final long serialVersionUID = 558720637535974546L;

    private RemoteException e;

    public RemoteDataAccessException(RemoteException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    public RemoteException getRemoteException() {
        return this.e;
    }
}
