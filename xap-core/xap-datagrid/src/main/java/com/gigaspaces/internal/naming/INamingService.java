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

package com.gigaspaces.internal.naming;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceItemFilter;

import java.rmi.RemoteException;

/**
 * The <code>INamingService</code> interface provides methods: <ul> <li> For obtaining references to
 * remote services in a remote naming service.<br> Each method of the <code>INamingService</code>
 * takes as one of its arguments a {@link net.jini.core.lookup.ServiceTemplate} to lookup the
 * desired service. </li> <li> To control a service's participation in the join protocol.<br> The
 * object returned by the Administrable.getAdmin method should implement this interface, in addition
 * to any other service-specific administration interfaces. </li> </ul>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see Administrable
 * @since 6.0
 */
public interface INamingService {
    /**
     * Queries the naming service in the managed set for service(s) that match the input criteria.
     * Additionally, this method allows any entity to supply an object referred to as a
     * <i>filter</i>. This filtering facility is particularly useful to entities that wish to extend
     * the capabilities of standard template-matching. <p> Entities typically employ this method
     * when they need infrequent access to multiple instances of services, and when the cost of
     * making remote queries is outweighed by the overhead of maintaining a local cache (for
     * example, because of resource limitations). <p> This version of <code>lookup</code> returns an
     * <i>array</i> of instances of <code>ServiceItem</code> in which each element corresponds to a
     * service reference that satisfies the matching criteria. The number of elements in the
     * returned set will be no greater than the value of the <code>maxMatches</code> parameter, but
     * may be less. <p> Note that this version of <code>lookup</code> does not provide a
     * <i>blocking</i> feature. That is, this version will return immediately with whatever number
     * of service references it can find, up to the number indicated in the <code>maxMatches</code>
     * parameter. If no services matching the input criteria can be found on the first attempt, an
     * empty array is returned. It is important to understand this characteristic because there is a
     * common usage scenario that can cause confusion when this version of <code>lookup</code> is
     * used but fails to discover any instances of the expected service of interest. Suppose an
     * entity creates a service discovery manager and then immediately calls this version of
     * <code>lookup</code>, which simply queries the currently discovered lookup services for the
     * service of interest. If the discovery manager employed by the service discovery manager has
     * not yet disovered any lookup services (thus, there are no lookup services to query) the
     * method will immediately return an empty array. This can be confusing when one verifies that
     * instance(s) of such a service of interest have indeed been started and registered with the
     * existing lookup service(s). To address this issue, one of the blocking versions of
     * <code>lookup</code> could be used instead of this version, or the entity could simply wait
     * until the discovery manager has been given enough time to complete its own (lookup) discovery
     * processing.
     *
     * @param tmpl       an instance of <code>ServiceTemplate</code> corresponding to the object to
     *                   use for template-matching when searching for desired services. If
     *                   <code>null</code> is input to this parameter, this method will use a
     *                   <i>wildcarded</i> template (will match all services) when performing
     *                   template-matching. Note that the effects of modifying contents of this
     *                   parameter before this method returns are unpredictable and undefined.
     * @param maxMatches this method will return no more than this number of service references
     * @param filter     an instance of <code>ServiceItemFilter</code> containing matching criteria
     *                   that should be applied in addition to the template-matching employed when
     *                   searching for desired services. If <code>null</code> is input to this
     *                   parameter, then only template-matching will be employed to find the desired
     *                   services.
     * @return an array of instances of <code>ServiceItem</code> where each element corresponds to a
     * reference to a service that matches the criteria represented in the input parameters; or an
     * empty array if no matching service can be found; <code>null</code> if no naming service can
     * be found.
     * @see net.jini.core.lookup.ServiceTemplate
     * @see net.jini.lookup.ServiceItemFilter
     **/
    ServiceItem[] lookup(ServiceTemplate srvTemplate, int maxMatches, ServiceItemFilter filter);


    /**
     * The <code>notify</code> method allows the client-like entity to request  create a new managed
     * set (or cache) and populate it with services, which match criteria defined by the entity, and
     * whose references are registered with one or more of the lookup services the entity has
     * targeted for discovery. <p> This method returns an object of type <code>LookupCache</code>.
     * Through this return value, the entity can query the cache for services of interest, manage
     * the cache's event mechanism for service discoveries, or terminate the cache. <p> An entity
     * typically uses the object returned by this method to provide local storage of, and access to,
     * references to services that it is interested in using. Entities needing frequent access to
     * numerous services will find the object returned by this method quite useful because
     * acquisition of those service references is provided through local method invocations.
     * Additionally, because the object returned by this method provides an event mechanism, it is
     * also useful to entities wishing to simply monitor, in an event-driven manner, the state
     * changes that occur in the services of interest. <p> Although not required, a common usage
     * pattern for entities that wish to use the <code>LookupCache</code> class to store and manage
     * "discovered" services is to create a separate cache for each service type of interest.
     *
     * @param tmpl     template to match. It uses template-matching semantics to identify the
     *                 service(s) to acquire from lookup services in the managed set. If this value
     *                 is <code>null</code>, it is the equivalent of passing a
     *                 <code>ServiceTemplate</code> constructed with all <code>null</code> arguments
     *                 (all wildcards).
     * @param filter   used to apply additional matching criteria to any <code>ServiceItem</code>
     *                 found through template-matching. If this value is <code>null</code>, no
     *                 additional filtering will be applied beyond the template-matching.
     * @param listener object that will receive notifications when services matching the input
     *                 criteria are discovered for the first time, or have encountered a state
     *                 change such as removal from all lookup services or attribute set changes. If
     *                 this value is <code>null</code>, the cache resulting from that invocation
     *                 will send no such notifications.
     * @return LookupCache used to query the cache for services of interest, manage the cache's
     * event mechanism for service discoveries, or terminate the cache.
     * @throws java.rmi.RemoteException typically, this exception occurs when a RemoteException
     *                                  occurs as a result of an attempt to export the remote
     *                                  listener that receives service events from the lookup
     *                                  services in the managed set.
     * @see net.jini.lookup.ServiceItemFilter
     **/
    LookupCache notify(ServiceTemplate tmpl, ServiceItemFilter filter, ServiceDiscoveryListener listener)
            throws RemoteException;


    /**
     * Get the current attribute sets for the service.
     *
     * @param service the service to get attributes from.
     * @return the current attribute sets for the service
     **/
    Entry[] getLookupAttributes(Object service) throws RemoteException;


    /**
     * Modify the current attribute sets for desired service. The resulting set will be used for all
     * future joins. The same modifications are also made to all currently-joined naming services.
     *
     * @param service          the service to modify the <code>attrSet</> by matched
     *                         <code>attrSetTemplates</codes>.
     * @param attrSetTemplates the templates for matching attribute sets
     * @param attrSets         the modifications to make to matching sets
     **/
    void modifyNamingAttributes(Object service, Entry[] attrSetTemplates, Entry[] attrSets)
            throws RemoteException;

    /**
     * Add attribute sets for the service.  The resulting set will be used for all future joins. The
     * attribute sets are also added to all currently-joined lookup services.
     *
     * @param attrSets the attribute sets to add
     **/
    void addNamingAttributes(Object service, Entry[] attrSets)
            throws RemoteException;

    /**
     * @return The number of registrars available for querying by the naming service
     */
    int getNumberOfRegistrars();

    /**
     * Terminate the naming service functionality.
     **/
    void terminate();

    /**
     * @return The Naming Service name.
     **/
    String getName();
}
