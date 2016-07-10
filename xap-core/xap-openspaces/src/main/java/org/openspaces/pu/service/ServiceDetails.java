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

import java.io.Serializable;
import java.util.Map;

/**
 * A generic service that exists within a processing unit.
 *
 * @author kimchy
 * @see org.openspaces.pu.service.PlainServiceDetails
 */
public interface ServiceDetails extends Serializable {

    /**
     * Returns the id of the processing unit (usually the bean id).
     */
    String getId();

    /**
     * Returns the service type. For example, space, dotnet, jee.
     */
    String getServiceType();

    /**
     * Returns the type of the serive details. For example, in case of space, it can be localcache,
     * proxy, ... .
     */
    String getServiceSubType();

    /**
     * Returns a short description of the service.
     */
    String getDescription();

    /**
     * Returns the long description
     */
    String getLongDescription();

    /**
     * Returns extra attributes the service details wishes to expose.
     */
    Map<String, Object> getAttributes();

//    /**
//     * Aggregates an array of service details into an aggregated view of it. All service details are of the same
//     * service type. Can return <code>null</code> if no aggregation can be performed.
//     */
//    AggregatedServiceDetails aggregateByServiceType(ServiceDetails[] servicesDetails);
//
//    /**
//     * Aggregates an array of service details into an aggregated view of it. All service details are of the same
//     * service type and service sub type. Can return <code>null</code> if no aggregation can be performed.
//     */
//    AggregatedServiceDetails aggregateByServiceSubType(ServiceDetails[] servicesDetails);
//
//    /**
//     * Aggregates an array of service details into an aggregated view of it. All service details are of the same
//     * id (and service type and service sub type). Can return <code>null</code> if no aggregation can be performed.
//     */
//    AggregatedServiceDetails aggregateById(ServiceDetails[] servicesDetails);
}
