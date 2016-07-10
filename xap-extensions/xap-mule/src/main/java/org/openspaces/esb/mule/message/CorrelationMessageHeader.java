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

package org.openspaces.esb.mule.message;

import org.mule.api.config.MuleProperties;

/**
 * Base interface that expose mule meta data. and explicitly expose Correlation attributes.
 *
 * <p> <B>Note:</B> implementation of this interface must have consistent results with equivalent
 * get/set Property method.
 *
 * @author yitzhaki
 */
public interface CorrelationMessageHeader extends MessageHeader {

    public static String CORRELATION_ID = MuleProperties.MULE_CORRELATION_ID_PROPERTY;

    public static String CORRELATION_SEQUENCE = MuleProperties.MULE_CORRELATION_SEQUENCE_PROPERTY;

    public static String CORRELATION_GROUP_SIZE = MuleProperties.MULE_CORRELATION_GROUP_SIZE_PROPERTY;


    String getCorrelationId();

    void setCorrelationId(String correlationId);

    Integer getCorrelationSequence();

    void setCorrelationSequence(Integer correlationSequence);

    Integer getCorrelationGroupSize();

    void setCorrelationGroupSize(Integer correlationGroupSize);

}
