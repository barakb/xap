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


package org.openspaces.core.space;

import com.j_spaces.core.IJSpace;

import org.springframework.dao.DataAccessException;

/**
 * A direct space factory bean, initialized with an existing {@link IJSpace} and provides it as the
 * space.
 *
 * <p>Though mostly not relevant for xml based configuration, this might be relevant when using
 * programmatic configuration.
 *
 * @author kimchy
 * @see UrlSpaceFactoryBean
 */
public class DirectSpaceFactoryBean extends AbstractSpaceFactoryBean {

    private IJSpace space;

    /**
     * Constucts a new direct space factory using the provided space.
     *
     * @param space The space to use
     */
    public DirectSpaceFactoryBean(IJSpace space) {
        this.space = space;
    }

    /**
     * Returns the space provided in the constructor.
     *
     * @see AbstractSpaceFactoryBean#doCreateSpace()
     */
    protected IJSpace doCreateSpace() throws DataAccessException {
        return space;
    }
}
