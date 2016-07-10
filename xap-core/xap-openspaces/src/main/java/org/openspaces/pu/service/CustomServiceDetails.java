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

package org.openspaces.pu.service;


/**
 * This class should be used by customers in order to implement their ServiceDetails
 *
 * @since 8.0.1
 */
public class CustomServiceDetails extends PlainServiceDetails {
    public static final String SERVICE_TYPE = "custom-details";
    private static final long serialVersionUID = -8038713604075604209L;

    // Just for externalizable
    public CustomServiceDetails() {
    }

    /**
     * @param id should identify that service
     * @deprecated since 9.0 Use {@link #CustomServiceDetails(String, String, String, String)}
     * instead - constructor that does not receive service type as parameter since custom type is
     * always {@link #SERVICE_TYPE}
     */
    public CustomServiceDetails(String id, String serviceType, String serviceSubType,
                                String description, String longDescription) {

        super(id, serviceType, serviceSubType, description, longDescription);
    }

    /**
     * Constructor
     *
     * @param id          should identify that service, should be same as {@link
     *                    CustomServiceMonitors}'s id
     * @param description should be same as {@link CustomServiceMonitors}'s description
     */
    public CustomServiceDetails(String id, String serviceSubType, String description,
                                String longDescription) {

        super(id, SERVICE_TYPE, serviceSubType, description, longDescription);
    }
}
