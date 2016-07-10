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


package org.openspaces.pu.container.jee;

import org.springframework.beans.factory.FactoryBean;

/**
 * A port generator that accepts a basePort and a portOffest and simply adds them.
 *
 * @author kimchy
 */
public class PortGenerator implements FactoryBean {

    private int basePort = 0;

    private int portOffset = 0;

    public void setBasePort(int basePort) {
        this.basePort = basePort;
    }

    public void setPortOffset(int portOffset) {
        this.portOffset = portOffset;
    }

    public int getPort() {
        return basePort + portOffset;
    }

    public Object getObject() throws Exception {
        return getPort();
    }

    public Class getObjectType() {
        return Integer.class;
    }

    public boolean isSingleton() {
        return true;
    }
}
