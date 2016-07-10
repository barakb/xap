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

import com.gigaspaces.client.WriteModifiers;

import net.jini.core.lease.Lease;


/**
 * LeaseContext is a return-value encapsulation of a write operation. <p> Using either {@link
 * IJSpace#write(Object, net.jini.core.transaction.Transaction, long)} or {@link
 * IJSpace#write(Object, net.jini.core.transaction.Transaction, long, long, int)} with the {@link
 * com.j_spaces.core.client.UpdateModifiers#UPDATE_OR_WRITE} returns a LeaseContext according to
 * UPDATE-OR-WRITE semantics. The update-or-write operation is atomic - either a write or an update
 * is performed. <p> <code>LeaseContext's</code> {@link #getObject()} returns: <ul> <li>null, on a
 * successful write <li>previous value, on successful update </ul>
 *
 * <code><pre>
 * //first write
 * LeaseContext lease = space.write((Object)o, null, 1000);
 * lease.getUID(); //returns UID of written object
 * lease.getOriginalObject(); //returns null on first write
 * lease.renew(Lease.FOREVER);
 *
 * //second write - equivalent to update
 * lease = space.write((Object)o, null, 1000);
 * lease.getUID(); //returns UID of object
 * lease.getOriginalObject(); //returns previous value (i.e. of previous write)
 * try{
 *   lease.renew(Lease.FOREVER);
 * }catch(UnsupportedOperationException e) {
 *  //not a real lease - constructed by update.
 * }
 *
 * </code> </pre>
 *
 * @author Lior Ben Yizhak
 * @version 5.0
 */
public interface LeaseContext<T> extends Lease {
    /**
     * Returns UID of written Entry.
     *
     * @return UID of written Entry.
     **/
    public String getUID();

    /**
     * Returns the version of written \ updated entry.
     *
     * @return version of written \ updated entry.
     * @since 9.0.0
     */
    public int getVersion();

    /**
     * returns the previous value associated with this update-or-write operation. <ul> <li>null, on
     * a successful write </ul> <li>previous value, on successful update (Only if {@link
     * WriteModifiers#RETURN_PREV_ON_UPDATE} is used, otherwise null)
     *
     * @return the associated object.
     */
    public T getObject();
}
