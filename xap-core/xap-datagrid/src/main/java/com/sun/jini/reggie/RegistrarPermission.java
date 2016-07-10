/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.reggie;

import net.jini.security.AccessPermission;

/**
 * An instance contains a name (also referred to as a "target name") but no actions list; you either
 * have the named permission or you don't.  The convention is that the target name is the fully
 * qualified name of the remote method being invoked.  Wildcard matches are supported using the
 * syntax specified by {@link AccessPermission}. <p> The possible target names for use with a Reggie
 * server are specified in the package documentation for {@link com.sun.jini.reggie}.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class RegistrarPermission extends AccessPermission {

    private static final long serialVersionUID = 2L;

    /**
     * Creates an instance with the specified target name.
     *
     * @param name the target name
     * @throws NullPointerException     if the target name is <code>null</code>
     * @throws IllegalArgumentException if the target name does not match the syntax specified in
     *                                  the comments at the beginning of the {@link
     *                                  AccessPermission} class
     */
    public RegistrarPermission(String name) {
        super(name);
    }
}
