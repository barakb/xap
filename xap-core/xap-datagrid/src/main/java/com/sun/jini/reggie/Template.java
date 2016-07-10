/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.reggie;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceTemplate;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;

/**
 * A Template contains the fields of a ServiceTemplate packaged up for transmission between
 * client-side proxies and the registrar server. Instances are never visible to clients, they are
 * private to the communication between the proxies and the server. <p> This class only has a bare
 * minimum of methods, to minimize the amount of code downloaded into clients.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class Template implements Serializable {

    private static final long serialVersionUID = 2L;

    /**
     * ServiceTemplate.serviceID
     *
     * @serial
     */
    public ServiceID serviceID;
    /**
     * ServiceTemplate.serviceTypes converted to ServiceTypes
     *
     * @serial
     */
    public ServiceType[] serviceTypes;
    /**
     * ServiceTemplate.attributeSetTemplates converted to EntryReps
     *
     * @serial
     */
    public EntryRep[] attributeSetTemplates;

    Template(ServiceID serviceID, ServiceType[] serviceTypes, EntryRep[] attributeSetTemplates) {
        this.serviceID = serviceID;
        this.serviceTypes = serviceTypes;
        this.attributeSetTemplates = attributeSetTemplates;
    }

    /**
     * Converts a ServiceTemplate to a Template.  Any exception that results is bundled up into a
     * MarshalException.
     */
    public Template(ServiceTemplate tmpl) throws RemoteException {
        serviceID = tmpl.serviceID;
        serviceTypes = ClassMapper.toServiceType(tmpl.serviceTypes);
        attributeSetTemplates =
                EntryRep.toEntryRep(tmpl.attributeSetTemplates, false);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Template template = (Template) o;

        if (serviceID != null ? !serviceID.equals(template.serviceID) : template.serviceID != null)
            return false;
        if (!Arrays.equals(attributeSetTemplates, template.attributeSetTemplates)) return false;
        if (!Arrays.equals(serviceTypes, template.serviceTypes)) return false;

        return true;
    }

    public int hashCode() {
        int result;
        result = (serviceID != null ? serviceID.hashCode() : 0);
        result = 31 * result + (serviceTypes != null ? arrayHash(serviceTypes, false) : 0);
        result = 31 * result + (attributeSetTemplates != null ? arrayHash(attributeSetTemplates, true) : 0);
        return result;
    }

    private static int arrayHash(Object a[], boolean useIndexFactor) {
        if (a == null)
            return 0;

        int result = 1;

        for (int i = 0; i < a.length; i++)
            result = (31 * result * (useIndexFactor ? (i + 1) : 1)) + (a[i] == null ? 0 : a[i].hashCode());

        return result;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Template [").append(hashCode()).append("] {");
        sb.append(serviceID).append(",").append("[");
        asList(serviceTypes, sb);
        sb.append("], [");
        asList(attributeSetTemplates, sb);
        sb.append("]}");
        return sb.toString();
    }

    private void asList(Object[] a, StringBuffer sb) {
        if (a == null) {
            sb.append("null");
            return;
        }
        for (int i = 0; i < a.length; i++) {
            sb.append(a[i]).append(",");
        }
    }
}
