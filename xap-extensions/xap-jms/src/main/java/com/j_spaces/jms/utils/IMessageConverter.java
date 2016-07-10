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

package com.j_spaces.jms.utils;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * This interface is used by JMS layer to define what the JMS layer writes to the space.
 */
public interface IMessageConverter {
    /**
     * The object returned by this method will be written to the space.
     *
     * @param msg the JMS message to convert.
     * @return the Object we want to write to the space.
     * @throws JMSException if there was a problem during the conversion.
     */
    public Object toObject(Message msg) throws JMSException;
}
