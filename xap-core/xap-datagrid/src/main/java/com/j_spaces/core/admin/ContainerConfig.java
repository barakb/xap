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

package com.j_spaces.core.admin;

import com.j_spaces.core.filters.GenericPrincipal;
import com.j_spaces.core.filters.UserDefinedRole;

import java.io.Serializable;
import java.util.Map;

/**
 * This structure contains all information about container configuration.
 * <code>ContainerConfig</code> builds inside of Server and transfered to the side of client.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.j_spaces.core.admin.IJSpaceContainerAdmin#getConfig()
 */
@com.gigaspaces.api.InternalApi
public class ContainerConfig
        implements Serializable, Cloneable {
    /**
     * use serialVersionUID from GigaSpaces 5.0 for inter-operability.
     */
    static final long serialVersionUID = 2L;

    /***
     * CONTAINER CONFIGURATION  <containerName>-config.xml.
     ***/
    public boolean updateModeEnabled = false;
    public String homeDir;
    public String containerHostName;
    public String jndiUrl;
    public String containerName;
    public String license;
    public String lookupGroups;
    public boolean unicastEnabled = false;
    public String unicastURL;

    // registration modes for build containerConfig
    final public static int JNDI_MODE = 1;
    final public static int LUS_MODE = 2;
    final public static int JNDI_MODE_AND_LUS_MODE = JNDI_MODE + LUS_MODE;

    public final static int WEB_CONTAINER_MODE_JINI = 0;
    public final static int WEB_CONTAINER_MODE_TOMCAT = 1;

    // JMS Settings
    public boolean jmsEnabled;
    public boolean jmsInternalJndiEnabled;
    public boolean jmsExtJndiEnabled;

    private String _schemaName;
    private boolean _isShutdownHook;
    private boolean _isHttpdEnabled;
    private String _httpdAdditionalRoots;
    private String _httpdExplicitBindingAddress;
    private String _httpdExplicitPort;
    private boolean _isJndiEnabled;
    private boolean _isJiniLusEnabled;
    private boolean _isStartEmbeddedJiniLus;
    private boolean _isStartEmbeddedJiniMahalo;
    private boolean _isJMXEnabled;


    //map of all defined schemas, key is schema name,
    //value is SpaceConfig instance
    private Map<String, SpaceConfig> _spaceSchemasMap;


    // TODO OLD SECURITY - NEED TO REMOVE IN NEXT MAJOR VERSION AFTER 7.0
    // Administrator account
    @Deprecated
    public String adminName;
    @Deprecated
    public String adminPassword;
    @Deprecated
    public String securityMode = SECURITY_FULL_CONTROL;
    @Deprecated
    public GenericPrincipal[] usersInfo;
    @Deprecated
    public UserDefinedRole[] userDefinedRoles;
    @Deprecated
    final public static String SECURITY_READ_ONLY = "READ_ONLY";
    @Deprecated
    final public static String SECURITY_FULL_CONTROL = "FULL_CONTROL";

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            // this shouldn't happen, since we are not Cloneable
            throw new InternalError();
        }
    }

    public Map<String, SpaceConfig> getSpaceSchemasMap() {
        return _spaceSchemasMap;
    }

    public void setSpaceSchemasMap(Map<String, SpaceConfig> spaceSchemasMap) {
        this._spaceSchemasMap = spaceSchemasMap;
    }

    public boolean isJiniLusEnabled() {
        return _isJiniLusEnabled;
    }

    public void setJiniLusEnabled(boolean isJiniLusEnabled) {
        this._isJiniLusEnabled = isJiniLusEnabled;
    }

    public boolean isJndiEnabled() {
        return _isJndiEnabled;
    }

    public void setJndiEnabled(boolean isJndiEnabled) {
        this._isJndiEnabled = isJndiEnabled;
    }

    public String getJndiURL() {
        return jndiUrl;
    }

    public boolean isShutdownHook() {
        return _isShutdownHook;
    }

    public void setShutdownHook(boolean isShutdownHook) {
        this._isShutdownHook = isShutdownHook;
    }

    public boolean isStartEmbeddedJiniMahalo() {
        return _isStartEmbeddedJiniMahalo;
    }

    public void setStartEmbeddedJiniMahalo(boolean isStartEmbeddedJiniMahalo) {
        this._isStartEmbeddedJiniMahalo = isStartEmbeddedJiniMahalo;
    }

    public boolean isStartEmbeddedJiniLus() {
        return _isStartEmbeddedJiniLus;
    }

    public void setStartEmbeddedJiniLus(boolean isStartEmbeddedJiniLus) {
        this._isStartEmbeddedJiniLus = isStartEmbeddedJiniLus;
    }

    public String getSchemaName() {
        return _schemaName;
    }

    public void setSchemaName(String schemaName) {
        this._schemaName = schemaName;
    }

    public boolean isJMXEnabled() {
        return _isJMXEnabled;
    }

    public void setJMXEnabled(boolean enabled) {
        _isJMXEnabled = enabled;
    }
}