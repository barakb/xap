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


package org.openspaces.jms;

import com.j_spaces.jms.GSConnectionFactoryImpl;
import com.j_spaces.jms.GSXAConnectionFactoryImpl;
import com.j_spaces.jms.utils.IMessageConverter;

import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * A Spring factory bean for creating GigaSpaces JMS implementation of JMS
 * <code>XAConnectionFactory</code> based on a Space instance.
 *
 * <p>Note, the {@link org.openspaces.core.GigaSpace} is used to acquire the {@link
 * com.j_spaces.core.IJSpace} instance allowing to get the "clustered" flag behavior. Transactional
 * support should use plain JMS transactional handling and GigaSpace support for automatic
 * transaction joining is not used in this case.
 *
 * @author kimchy
 */
public class GigaSpaceXAConnectionFactory implements FactoryBean, InitializingBean {

    private GigaSpace gigaSpace;

    private IMessageConverter messageConverter;


    private GSXAConnectionFactoryImpl gsConnectionFactory;


    /**
     * The GigaSpace instance to acquire the {@link com.j_spaces.core.IJSpace} from.
     *
     * <p>Note, the {@link org.openspaces.core.GigaSpace} is used to acquire the {@link
     * com.j_spaces.core.IJSpace} instance allowing to get the "clustered" flag behavior.
     * Transactional support should use plain JMS transactional handling and GigaSpace support for
     * automatic transaction joining is not used in this case.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * An optional message converter allowing (for sending purposes) not to send JMS messages to the
     * Space, but to send a converted instance and write a "business" entry to the Space.
     */
    public void setMessageConverter(IMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public void afterPropertiesSet() throws Exception {
        gsConnectionFactory = new GSXAConnectionFactoryImpl(gigaSpace.getSpace(), messageConverter);
    }

    public Object getObject() throws Exception {
        return gsConnectionFactory;
    }

    public Class getObjectType() {
        return gsConnectionFactory == null ? GSConnectionFactoryImpl.class : gsConnectionFactory.getClass();
    }

    public boolean isSingleton() {
        return true;
    }
}