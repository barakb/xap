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


package com.gigaspaces.client;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateOnceMap;
import com.j_spaces.core.client.Modifiers;

import java.util.Map;

/**
 * Provides modifiers to customize the behavior of write operations.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */

public class WriteModifiers extends SpaceProxyOperationModifiers {
    private static final long serialVersionUID = 1L;

    /**
     * Empty - use operation default behavior.
     */
    public static final WriteModifiers NONE = new WriteModifiers(Modifiers.NONE);

    /**
     * If the entry already exists in the space throw an EntryAlreadyInSpaceException.
     *
     * Notice: cannot be used in together with {@link #UPDATE_OR_WRITE}, {@link #UPDATE_ONLY},
     * {@link #PARTIAL_UPDATE}.
     *
     * @see {@link SpaceId}
     */
    public static final WriteModifiers WRITE_ONLY = new WriteModifiers(Modifiers.WRITE);

    /**
     * If the entry does not exists in the space throw an EntryNotInSpaceException.
     *
     * Notice: cannot be used in together with {@link #UPDATE_OR_WRITE}, {@link #WRITE_ONLY}.
     *
     * @see {@link SpaceId}
     */
    public static final WriteModifiers UPDATE_ONLY = new WriteModifiers(Modifiers.UPDATE);

    /**
     * If the entry does not exists in the space it is written, otherwise it is updated.
     *
     * Notice: cannot be used in together with {@link #WRITE_ONLY}, {@link #UPDATE_ONLY}.
     *
     * @see {@link SpaceId}
     */
    public static final WriteModifiers UPDATE_OR_WRITE = new WriteModifiers(Modifiers.UPDATE_OR_WRITE);

    /**
     * Update only non-null properties, ignore null properties.
     *
     * Notice: cannot be used in together with {@link #WRITE_ONLY}.
     *
     * @see {@link SpaceId}
     */
    public static final WriteModifiers PARTIAL_UPDATE = new WriteModifiers(Modifiers.PARTIAL_UPDATE);

    /**
     * If the entry already exists in the space, return the old entry in the result.
     */
    public static final WriteModifiers RETURN_PREV_ON_UPDATE = new WriteModifiers(Modifiers.RETURN_PREV_ON_UPDATE);

    /**
     * Operation is executed in one way mode, meaning no return value will be provided. Using this
     * mode provides no guarantee whether the operation succeeded or not, the only guarantee is that
     * the operation was successfully written to the local network buffer. As a result, using this
     * modifier will cause the operation not to guarantee automatic fail-over if the primary space
     * instance failed, and it cannot be done under a transaction. When used with an auto generated
     * id space entry, the local entry instance will not be updated with the generated id as this is
     * done on the server. The same applies for version property.
     */
    public static final WriteModifiers ONE_WAY = new WriteModifiers(Modifiers.ONE_WAY);


    /**
     * Look only in memory for existence of entry with the same ID - do not use the underlying
     * external data source. We assume that if an entry with the same ID does not reside in cache -
     * it does not exist in the underlying external data source either. When using this modifier
     * special care should be taken into consideration, it is the responsibility of the caller when
     * using this modifier to make sure the entry does not reside in the external data source,
     * failing to do so may cause inconsistent and fatal behavior of the system at a later point
     * when this entry is trying to be persisted into the external data source but it already exists
     * there.
     *
     * @since 9.1.1
     */
    public static final WriteModifiers MEMORY_ONLY_SEARCH = new WriteModifiers(Modifiers.MEMORY_ONLY_SEARCH);

    private static final Map<Integer, SpaceProxyOperationModifiers> cache = initCache();

    private static Map<Integer, SpaceProxyOperationModifiers> initCache() {
        Map<Integer, SpaceProxyOperationModifiers> initialValues = new CopyOnUpdateOnceMap<Integer, SpaceProxyOperationModifiers>();
        initialValues.put(NONE.getCode(), NONE);
        initialValues.put(WRITE_ONLY.getCode(), WRITE_ONLY);
        initialValues.put(UPDATE_ONLY.getCode(), UPDATE_ONLY);
        initialValues.put(UPDATE_OR_WRITE.getCode(), UPDATE_OR_WRITE);
        initialValues.put(PARTIAL_UPDATE.getCode(), PARTIAL_UPDATE);
        initialValues.put(RETURN_PREV_ON_UPDATE.getCode(), RETURN_PREV_ON_UPDATE);
        initialValues.put(ONE_WAY.getCode(), ONE_WAY);
        initialValues.put(MEMORY_ONLY_SEARCH.getCode(), MEMORY_ONLY_SEARCH);
        return initialValues;
    }

    public WriteModifiers() {
    }

    private WriteModifiers(int code) {
        super(code);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public WriteModifiers(WriteModifiers modifiers1, WriteModifiers modifiers2) {
        super(modifiers1, modifiers2);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public WriteModifiers(WriteModifiers modifiers1, WriteModifiers modifiers2, WriteModifiers modifiers3) {
        super(modifiers1, modifiers2, modifiers3);
    }

    /**
     * Creates a new modifiers from the specified modifiers.
     */
    public WriteModifiers(WriteModifiers... modifiers) {
        super(modifiers);
    }

    /**
     * Checks if the specified modifier is set.
     *
     * @return true if the specified modifier is set, false otherwise.
     */
    public boolean contains(WriteModifiers modifiers) {
        return super.contains(modifiers);
    }

    /**
     * Creates a new modifiers instance which is a union of the specified modifiers and this
     * instance.
     *
     * @param modifiers Modifiers to add.
     * @return A union of the current modifiers and the specified modifiers.
     */
    public WriteModifiers add(WriteModifiers modifiers) {
        return createIfNeeded(super.add(modifiers));
    }

    /**
     * Creates a new modifiers instance which excludes the specified modifiers from this instance.
     *
     * @param modifiers Modifiers to remove.
     * @return The modifiers from this instance without the modifiers from the specified instance.
     */
    public WriteModifiers remove(WriteModifiers modifiers) {
        return createIfNeeded(super.remove(modifiers));
    }

    /**
     * Checks if this instance contains the {@link #WRITE_ONLY} setting.
     *
     * @return true if this instance contains the {@link #WRITE_ONLY} setting, false otherwise.
     */
    public boolean isWriteOnly() {
        return contains(WRITE_ONLY);
    }

    /**
     * Checks if this instance contains the {@link #UPDATE_ONLY} setting.
     *
     * @return true if this instance contains the {@link #UPDATE_ONLY} setting, false otherwise.
     */
    public boolean isUpdateOnly() {
        return contains(UPDATE_ONLY);
    }

    /**
     * Checks if this instance contains the {@link #UPDATE_OR_WRITE} setting.
     *
     * @return true if this instance contains the {@link #UPDATE_OR_WRITE} setting, false otherwise.
     */
    public boolean isUpdateOrWrite() {
        return contains(UPDATE_OR_WRITE);
    }

    /**
     * Checks if this instance contains the {@link #PARTIAL_UPDATE} setting.
     *
     * @return true if this instance contains the {@link #PARTIAL_UPDATE} setting, false otherwise.
     */
    public boolean isPartialUpdate() {
        return contains(PARTIAL_UPDATE);
    }

    /**
     * Checks if this instance contains the {@link #RETURN_PREV_ON_UPDATE} setting.
     *
     * @return true if this instance contains the {@link #RETURN_PREV_ON_UPDATE} setting, false
     * otherwise.
     */
    public boolean isReturnPrevOnUpdate() {
        return contains(RETURN_PREV_ON_UPDATE);
    }

    @Override
    protected WriteModifiers create(int modifiers) {
        return new WriteModifiers(modifiers);
    }

    @Override
    protected Map<Integer, SpaceProxyOperationModifiers> getCache() {
        return cache;
    }
}
