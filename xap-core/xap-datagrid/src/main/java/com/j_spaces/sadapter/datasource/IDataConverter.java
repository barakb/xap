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

package com.j_spaces.sadapter.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.UnknownTypeException;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.RemoteException;

/**
 * Converts user objects to internal representations.
 *
 * @param <I> internal space type e.g. IEntryPacket, EntryHolder...
 * @author Asy
 * @since 6.5
 */
public interface IDataConverter<I> {
    /**
     * Converts from internal representation
     *
     * @param insternalPacket internal representation
     * @return The original user Entry Object
     */
    Object toObject(I insternalPacket);

    /**
     * Converts to internal representation
     *
     * @param obj The object to convert to internal representation
     * @return The converted object in internal representation format
     */
    I toInternal(Object obj) throws RemoteException, UnusableEntryException, UnknownTypeException;

    SpaceDocument toDocument(I entryPacket);

}
