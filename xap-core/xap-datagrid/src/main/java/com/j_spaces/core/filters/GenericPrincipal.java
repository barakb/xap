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

import com.j_spaces.core.filters.DefaultSecurityFilter.MatchObject;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GenericPrincipal implementation of {@link Principal} that is available for use by {@link
 * com.j_spaces.core.filters.ISpaceUserAccountDriver ISpaceUserAccountDriver} implementations.
 *
 * @author Igor Goldenberg
 * @version 2.1 $Date: 2007-02-21 10:15:23 $
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class GenericPrincipal
        implements Principal, Serializable {
    public static class Attribute implements Serializable {
        private static final long serialVersionUID = 8028144157679424915L;

        public String name;
        public Object value;
        private transient int hash;
        private boolean compareValue;

        public Attribute() {
        }

        public Attribute(String name, Object value) {
            this.name = name;
            this.value = value;
            compareValue = true;
        }

        /**
         * Returns true if names of the attributes are the same.
         *
         * @param o another <code>Attribute</code> instance
         * @return if the two Attribute instances are the same
         */
        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (o == this) return true;

            if (!(o instanceof Attribute)) return false;
            final Attribute attribute = (Attribute) o;

            if (name != null ? !name.equals(attribute.name) : attribute.name != null)
                return false;
            if (compareValue)
                if (value != null ? !value.equals(attribute.value) : attribute.value != null)
                    return false;
            return true;
        }

        /**
         * @return the value hashCode
         */
        @Override
        public int hashCode() {
            if (hash == 0) {
                hash = name.hashCode();
                if (value != null)
                    hash ^= value.hashCode();
            }
            return hash;
        }

        /**
         * @return "Attribute{name=" + name + ", value=" + value + '}'
         */
        @Override
        public String toString() {
            return "Attribute{name=" + name + ", value=" + value + '}';
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * The username of the user represented by this Principal.
     */
    String userName;

    /**
     * The authentication credentials for the user represented by  this Principal.
     */
    String password;

    /**
     * Indicate if this user should be hidden to security management on client side.
     */
    boolean isHidden;

    /** Indicate if this user shouldn't be saved */
    //boolean isTransient;

    /**
     * The set of roles associated with this user.
     */
    //either Admin, Read or/and Write
    String roles[] = new String[0];
    /**
     * List contains only user-defined roles.
     */
    public ArrayList<String> userDefinedRoles;

    /* List of permitted entries for Write role (key - entryType, value - list of Attributes)*/
    public Map<String, List<GenericPrincipal.Attribute>> writeEntries;
    public Map<String, List<GenericPrincipal.Attribute>> readEntries;
    public Map<String, List<GenericPrincipal.Attribute>> executeTasks;

    /* key - entryType, value - object (MatchObject) ready to matching with fields values array*/
    public Map<String, MatchObject> writeMatchObjects;
    public Map<String, MatchObject> readMatchObjects;
    public Map<String, MatchObject> executeMatchObjects;

    /**
     * Construct a new Principal for the specified username and password, with the specified role
     * names (as Strings).
     *
     * @param userName     The username of the user represented by this Principal
     * @param password     Credentials used to authenticate this user
     * @param roles        List of roles (must be Strings) possessed by this user
     * @param writeEntries List of permitted entries for write operation
     * @param readEntries  List of permitted entries for read operation
     * @param executeTasks List of permitted tasks for execute operation
     **/
    public GenericPrincipal(String userName, String password, List<String> roles,
                            Map<String, List<GenericPrincipal.Attribute>> writeEntries, Map<String,
            List<GenericPrincipal.Attribute>> readEntries, Map<String, List<GenericPrincipal.Attribute>> executeTasks) {
        this.userName = userName;
        this.password = password;
        this.writeEntries = writeEntries;
        this.readEntries = readEntries;
        this.executeTasks = executeTasks;

        if (roles != null) {
            this.roles = new String[roles.size()];
            this.roles = roles.toArray(this.roles);
            if (this.roles.length > 0)
                Arrays.sort(this.roles);
        }

        writeMatchObjects = new HashMap<String, MatchObject>();
        readMatchObjects = new HashMap<String, MatchObject>();
        executeMatchObjects = new HashMap<String, MatchObject>();

        userDefinedRoles = new ArrayList<String>();
    }/* constructor */

    /**
     * Construct a new Principal for the specified username and password.
     *
     * @param userName The username of the user represented by this Principal
     * @param password The <b>encrypted</b> value used to authenticate user. The encryption
     *                 algorithm is <tt>DES/ECB/PKCS5Padding</tt>.
     */
    public GenericPrincipal(String userName, String password) {
        this(userName, password, null);
    }

    public GenericPrincipal(String userName, String password, List<String> roles) {
        this(userName, password, roles, null, null, null);
    }

    public String getName() {
        return "DefaultPrincipal";
    }

    /**
     * Returns the username of the user represented by this Principal.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Returns the password of the user represented by this Principal.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets a new password (overriding the one given during construction.
     *
     * @param newPassword a new password of the user represented by this Principal.
     */
    public void setPassword(String newPassword) {
        this.password = newPassword;
    }

    /**
     * Returns set of roles associated with this user.
     **/
    public String[] getRoles() {
        return roles;
    }

    public void setRoles(String[] roles) {
        this.roles = roles;
    }

    @Override
    public String toString() {
        return userName;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     */
    @Override
    public boolean equals(Object another) {
        if (another == null)
            return false;

        if (!(another instanceof GenericPrincipal))
            return false;

        if (!getUserName().equals(((GenericPrincipal) another).getUserName()))
            return false;

        return true;
    }

    /**
     * Return <code>true</code> if user defined as hidden to security management on client side
     * (e.g. SpaceBrowser application), otherwise <code>false</code>.
     */
    public boolean isHidden() {
        return isHidden;
    }

    /**
     * Set user visibility level to security management
     */
    void setHidden(boolean b) {
        isHidden = b;
    }

    /**
     * Returns <code>true</code> if user is transient, otherwise <code>false</code>.
     */
    /*public boolean isTransient() {
        return isTransient;
    }

    void setTransient(boolean b) {
        isTransient = b;
    }*/
}/* Principal class */

