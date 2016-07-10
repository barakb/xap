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

package com.gigaspaces.persistency.qa.model;

import java.io.Serializable;

/**
 * User representation
 */
public class User implements Serializable {

    /**
     * default serial version id
     */
    private static final long serialVersionUID = 1L;

    private String username;

    public User() {
    }

    /**
     * Construct a User reference
     *
     * @param username user name
     */
    public User(String username) {
        if (username == null)
            throw new IllegalArgumentException("user's name can't be null");

        this.username = username;
    }

    /**
     * @return the username of this User instance.
     */
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /* @see java.lang.Object#toString() */
    @Override
    public String toString() {
        return username;
    }

    /* @see java.lang.Object#hashCode() */
    @Override
    public int hashCode() {
        return username.hashCode();
    }

    /* @see java.lang.Object#equals(java.lang.Object) */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof User))
            return false;

        User otherUser = (User) obj;

        // verify username
        if (this.username == null && otherUser.username != null)
            return false;
        else if (this.username != null
                && !this.username.equals(otherUser.username))
            return false;

        // all properties equal
        return true;
    }

}
