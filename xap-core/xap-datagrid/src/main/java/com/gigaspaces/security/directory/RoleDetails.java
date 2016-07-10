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


package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;
import com.gigaspaces.security.SecurityManager;

import java.io.Serializable;

/**
 * Role details to store role information accessed by the {@link RoleManager} and {@link
 * SecurityManager}. <p> Concrete implementations must take particular care to ensure the non-null
 * contract detailed for each method is enforced. <p> Concrete implementations should be immutable
 * (value object semantics, like a String). This is because the <code>RoleDetails</code> will be
 * stored in caches and as such multiple threads may use the same instance.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public interface RoleDetails extends Serializable {

    /**
     * Returns the granted authorities granted to the role. Cannot return <code>null</code>.
     *
     * @return the authorities (never <code>null</code>)
     */
    Authority[] getAuthorities();

    /**
     * Returns the role name used to identify this role. Cannot return <code>null</code>.
     *
     * @return the role name (never <code>null</code>)
     */
    String getRole();
}
