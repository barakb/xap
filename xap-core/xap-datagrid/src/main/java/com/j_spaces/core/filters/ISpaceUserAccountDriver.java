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

package com.j_spaces.core.filters;


/**
 * A read-only facade for an underlying security ISpaceUserAccountDriver used to authenticate
 * individual users, and identify the security roles associated with those users.
 *
 * @author Igor Goldenberg
 * @version 2.1 $Date: 2007-05-06 15:08:05 $
 */
@Deprecated
public interface ISpaceUserAccountDriver {

    /**
     * Initialize the driver according to the url and container name.
     *
     * @param url           tokenized url
     * @param containerName container name
     **/
    public void init(String url, String containerName)
            throws Exception;


    /**
     * Return the Principal associated with the specified username and credentials, if there is one;
     * if username unknown or password is wrong {@link java.lang.SecurityException
     * SecurityException} will be thrown.
     *
     * @param username Username of the Principal to look up
     * @param password Password or other credentials to use in authenticating this username.
     * @return principal associated with the specified username
     */
    public GenericPrincipal authenticate(String username, String password);
}
