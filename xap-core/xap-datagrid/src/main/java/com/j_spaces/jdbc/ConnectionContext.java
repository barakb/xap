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

package com.j_spaces.jdbc;

import com.j_spaces.core.SpaceContext;
import com.j_spaces.jdbc.driver.GConnection;

import java.io.Serializable;

/**
 * The context of the {@link GConnection}
 *
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class ConnectionContext implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     *
     */
    public static final String PASSWORD = "password";
    /**
     *
     */
    public static final String USER = "user";
    /**
     *
     */
    private String _sessionName;

    private SpaceContext spaceContext;


    /**
     *
     */
    public ConnectionContext(String sessionName) {
        this._sessionName = sessionName;
    }

    /**
     * @return the sessionName
     */
    public String getSessionName() {
        return _sessionName;
    }

    /**
     * @param sessionName the sessionName to set
     */
    public void setSessionName(String sessionName) {
        this._sessionName = sessionName;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((_sessionName == null) ? 0 : _sessionName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConnectionContext other = (ConnectionContext) obj;
        if (_sessionName == null) {
            if (other._sessionName != null)
                return false;
        } else if (!_sessionName.equals(other._sessionName))
            return false;
        return true;
    }

    public void setSpaceContext(SpaceContext spaceContext) {
        this.spaceContext = spaceContext;
    }

    public SpaceContext getSpaceContext() {
        return spaceContext;
    }


}