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

package org.openspaces.core.config.modifiers;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.SpaceProxyOperationModifiers;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.Constants;

/**
 * Base class for different modifier types beans.
 *
 * @param <T> a subclass of {@link SpaceProxyOperationModifiers}
 * @author Dan Kilman
 * @since 9.5
 */
abstract public class AbstractSpaceProxyOperationModifierFactoryBean<T extends SpaceProxyOperationModifiers>
        implements FactoryBean<T> {

    private final Class<T> objectType;

    private T modifier;

    public AbstractSpaceProxyOperationModifierFactoryBean(Class<T> objectType) {
        this.objectType = objectType;
    }

    /**
     * @param modifierName The modifier constant name as defined in the relevant Modifiers class.
     * @see WriteModifiers
     * @see ReadModifiers
     * @see TakeModifiers
     * @see ClearModifiers
     * @see ChangeModifiers
     * @see com.gigaspaces.client.CountModifiers
     */
    @SuppressWarnings("unchecked")
    public void setModifierName(String modifierName) {

        Constants constants = getConstants();

        if (!constants.getNames("").contains(modifierName)) {
            throw new IllegalArgumentException("Unknown modifier: " + modifierName + " for type: " + getObjectType());
        }

        modifier = (T) constants.asObject(modifierName);
    }

    @Override
    public Class<T> getObjectType() {
        return objectType;
    }

    @Override
    public T getObject() throws Exception {
        return modifier;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    abstract protected Constants getConstants();

}
