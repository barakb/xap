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

package com.gigaspaces.client.mutators;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.server.MutableServerEntry;
import com.gigaspaces.sync.change.ChangeOperation;

import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 9.1
 */
public abstract class SpaceEntryMutator implements Serializable, ChangeOperation {
    private static final long serialVersionUID = 1L;

    /**
     * Changes an entry. The provided {@link MutableServerEntry} is wrapping the actual object which
     * is kept in space, therefore it is crucial to understand when a value is retrieved from the
     * entry it points to the actual reference in the data structure of the space. The content of
     * this reference should not be changed as it will affect directly the object in space and will
     * break data integrity, transaction and visibility scoping (transaction abort will not restore
     * the previous value). Changing a value should always be done via the {@link
     * MutableServerEntry#setPathValue(String, Object)}. Moreover, if you want to change a property
     * within that value by invoking a method on that object (e.g. if the value is a list, adding an
     * item to the list) first you must clone the fetched value first, and invoke the method on the
     * cloned copy otherwise, for the reason explained before, you will change the existing data
     * structure in the space without going the proper data update path and break data integrity.
     * <p> Note that when using a replicated topology (e.g. backup space, gateway, mirror) the
     * change operation itself is replicated (and *NOT* the modified entry). Hence, it is imperative
     * that this method will always cause the exact same affect on the entry assuming the same entry
     * was provided, for example it should not rely on variables that may change between executions,
     * such as system time, random, machine name etc. If the operation is not structured that way,
     * the state can be inconsistent in the different locations after being replicated When creating
     * a custom change operation always have this in the back of your mind - "With great power comes
     * great responsibility".
     *
     * <p> Following is an example that reads propertyA and set its value into propertyB and returns
     * nothing.
     * <pre><code>
     * public Object change(MutableServerEntry entry)
     * {
     *     Object value entry.getPathValue("propertyA");
     *     entry.setPathValue("propertyB", newValue);
     *     return null;
     * }
     * </code></pre>
     * <p> Following is an example that adds the element 2 into an ArrayList that exists in the
     * entry under a property named "listProperty", the result sent to client if requested is the
     * size of the collection after the change, note that we clone the ArrayList before modifying it
     * as explained before.
     * <pre><code>
     * public Object change(MutableServerEntry entry)
     * {
     *     ArrayList oldValue = (ArrayList)entry.getPathValue("listPropery");
     *     if (oldValue == null)
     *         throw new IllegalStateException("No collection instance exists under the given path
     * 'listProperty', in order to add a value a collection instance must exist");
     *     Collection newValue = (ArrayList)oldValue.clone()
     *     newValue.add(2);
     *     int size = newValue.size();
     *     entry.setPathValue("listProperty", newValue);
     *     return size;
     * }
     * </code></pre>
     *
     * @param entry the entry to change.
     * @return a result value of this change which will be sent to the client if the change
     * operation was executed with {@link ChangeModifiers#RETURN_DETAILED_RESULTS} modifier.
     */
    public abstract Object change(MutableServerEntry entry);

    public ChangeOperationResult getChangeOperationResult(final Serializable result) {
        return new ChangeOperationResult() {

            @Override
            public Serializable getResult() {
                return result;
            }

            @Override
            public ChangeOperation getOperation() {
                return SpaceEntryMutator.this;
            }
        };
    }
}
