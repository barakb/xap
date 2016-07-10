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

package com.j_spaces.core.filters;

import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceSecurityException;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.log.JProperties;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * On Init of DefaultSecurityFilter reads a encrypted binary file to configure the valid users,
 * passwords, and roles. This filter will called only when com.j_spaces.core.IJSpace#setSecurityContext(SecurityContext)
 * is set. If the userID not found or password is wrong {@link SecurityException} will be thrown.
 *
 * @author Igor Goldenberg
 * @version 1.2
 * @see SecurityException
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class DefaultSecurityFilter
        implements ISpaceFilter {
    /**
     * TODO This workaround is dedicated to mechanism like DCache, JMS, multicast notifications and
     * other that should be operate with space entries (mostly is com.j_spaces.core.client.MetaDataEntry)
     * for internal purposes, to make working properly with security layer.
     */
    private final Set<String> adminEntries;
    private final static String[] ADMINISTRATIVE_ENTRIES = {
            com.j_spaces.map.Envelope.class.getName(),
            "com.j_spaces.jms.JMSAckDataEntry",
            "com.j_spaces.jms.JMSDurableSubDataEntry",
            "com.j_spaces.jms.JMSOfflineStateDurSubDataEntry",
            "com.j_spaces.jms.JMSSessionDataEntry"
    };

    /**
     * Structure used to maintaining restricted values and their indexes.
     */
    static public class MatchObject {
        private List<String> subclasses;
        private List[] value; // on each position contains available list values
        private int[] index;
        private RuntimeException fault = null;
        private boolean isPrivilege = false;

        /**
         * Map of restriction contains key - array index, value - list of available values.
         */
        public MatchObject(List<String> subclasses, Map<Integer, List> grant, RuntimeException fault) {
            this.setSubclasses(subclasses);
            this.setFault(fault);
            int size = grant.size();
            setValue(new List[size]);
            setIndex(new int[size]);
            int i = 0;
            for (Map.Entry<Integer, List> entry : grant.entrySet()) {
                getIndex()[i] = entry.getKey().intValue();
                getValue()[i++] = entry.getValue();
            }
        }

        public MatchObject(RuntimeException fault) {
            this.setFault(fault);
        }

        public MatchObject(boolean isPrivilege) {
            this.setPrivilege(isPrivilege);
        }

        @Override
        public String toString() {
            return Arrays.asList(getValue()).toString();
        }

        private void setSubclasses(List<String> subclasses) {
            this.subclasses = subclasses;
        }

        public List<String> getSubclasses() {
            return subclasses;
        }

        private void setValue(List[] value) {
            this.value = value;
        }

        public List[] getValue() {
            return value;
        }

        private void setIndex(int[] index) {
            this.index = index;
        }

        public int[] getIndex() {
            return index;
        }

        private void setFault(RuntimeException fault) {
            this.fault = fault;
        }

        public RuntimeException getFault() {
            return fault;
        }

        private void setPrivilege(boolean isPrivilege) {
            this.isPrivilege = isPrivilege;
        }

        public boolean isPrivilege() {
            return isPrivilege;
        }
    }

    /**
     * Write operation.
     */
    public static final String OPERATION_WRITE = "write";
    /**
     * Read operation.
     */
    public static final String OPERATION_READ = "read";
    /**
     * Take operation.
     */
    public static final String OPERATION_TAKE = "take";
    /**
     * Update operation.
     */
    public static final String OPERATION_UPDATE = "update";
    /**
     * Notify operation.
     */
    public static final String OPERATION_NOTIFY = "notify";
    /**
     * Clean operation.
     */
    public static final String OPERATION_CLEAN = "clean";

    /**
     * execute operation
     */
    public static final String OPERATION_EXECUTE = "execute";

    /**
     * GigaSpaces MemoryRealm.
     */
    protected ISpaceUserAccountDriver m_realm;
    /**
     * Embedded space proxy.
     */
    protected IJSpace m_spaceProxy;

    /**
     * Empty constructor.
     */
    public DefaultSecurityFilter() {
        adminEntries = new HashSet<String>();
        for (int i = 0; i < ADMINISTRATIVE_ENTRIES.length; i++) {
            adminEntries.add(ADMINISTRATIVE_ENTRIES[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(IJSpace space, String filterId, String url, int priority)
            throws RuntimeException {
        m_spaceProxy = space;
        try {
            String containerName = space.getContainerName();
            initSpaceUserAccountDriver(space.getName(), url, containerName);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * For internal use only.
     */
    public void initSpaceUserAccountDriver(String spaceName, String url, String containerName)
            throws Exception {
        String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
        // get from XML the className of MemoryRealm driver
        String driverClassName = JProperties.getSpaceProperty(fullSpaceName,
                "filters.userAccountDriver", "com.j_spaces.core.filters.MemoryRealm");

        // create instance of driver
        m_realm = (ISpaceUserAccountDriver) ClassLoaderHelper.loadClass(driverClassName).newInstance();

        String rootDir = MemoryRealm.getRootDir();
        String urlPath = MemoryRealm.getUrlPath(url);
        String fileURL = rootDir + urlPath;
        URL securityFileURL = ResourceLoader.getResourceURL(urlPath, rootDir);
        if (securityFileURL != null) {
            fileURL = securityFileURL.toURI().getPath();
        }

        // init the defined MemoryRealm driver, url - is a name of user's permission file
        m_realm.init(fileURL, containerName);
    }

    /**
     * Called only on before replace. And after replace and update.
     */
    @Override
    public void process(SpaceContext context, ISpaceFilterEntry[] subject, int operationCode)
            throws RuntimeException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(SpaceContext context, ISpaceFilterEntry subject, int operationCode)
            throws RuntimeException {
        //        // TODO see workaround for ADMINISTRATIVE ENTRIES described above
        //        if (subject != null && adminEntries.contains(subject.getClassName()))
        //            return;
        //
        //        if (context == null || context.getSecurityContext() == null)
        //            throw new SpaceSecurityException("Access denied! No SecurityContext found.");
        //
        //        String userName = context.getSecurityContext().getUsername();
        //        String password = context.getSecurityContext().getPassword();
        //
        //        // if the userID and password are right, returns the credential of this user
        //        // otherwise SpaceSecurityException will be thrown
        //        GenericPrincipal userInfo =  m_realm.authenticate(userName, password);
        //
        //        switch (operationCode) {
        //        case FilterOperationCodes.SET_SECURITY:
        //            if (userInfo.getRoles().length > 0) {
        //                context.getSecurityContext().setPermissions(createSecurityContextRoles(userInfo.getRoles()));
        //            }
        //            break;
        //
        //            // WRITE ROLE
        //        case FilterOperationCodes.BEFORE_WRITE:
        //            checkPermissions(userInfo.userName, userInfo.writeEntries, userInfo.writeMatchObjects, subject, OPERATION_WRITE);
        //            break;
        //
        //        case FilterOperationCodes.BEFORE_TAKE:
        //        case FilterOperationCodes.BEFORE_TAKE_MULTIPLE:
        //            checkPermissions(userInfo.userName, userInfo.writeEntries, userInfo.writeMatchObjects, subject, OPERATION_TAKE);
        //            break;
        //
        //        case FilterOperationCodes.BEFORE_UPDATE:
        //            checkPermissions(userInfo.userName, userInfo.writeEntries, userInfo.writeMatchObjects, subject, OPERATION_UPDATE);
        //            break;
        //
        //            // READ ROLE
        //        case FilterOperationCodes.BEFORE_READ:
        //        case FilterOperationCodes.BEFORE_READ_MULTIPLE:
        //            checkPermissions(userInfo.userName, userInfo.readEntries, userInfo.readMatchObjects, subject, OPERATION_READ);
        //            break;
        //
        //        case FilterOperationCodes.BEFORE_NOTIFY:
        //            checkPermissions(userInfo.userName, userInfo.readEntries, userInfo.readMatchObjects, subject, OPERATION_NOTIFY);
        //            break;
        //
        //        case FilterOperationCodes.BEFORE_CLEAN_SPACE:
        //            // general write permissions are checked by the proxy.
        //            // check if this user has additional user defined restrictions on write operations
        //            // if so clean can't be performed
        //            if (userInfo.writeEntries != null && userInfo.writeEntries.size() > 0) {
        //                throw new SpaceSecurityException(OPERATION_CLEAN +
        //                        " operation doesn't allowed to user: " + userName);
        //            }
        //            break;
        //
        //        case FilterOperationCodes.BEFORE_EXECUTE:
        //            checkPermissions(userInfo.userName, userInfo.executeTasks, userInfo.executeMatchObjects, subject, OPERATION_EXECUTE);
        //            break;
        //
        //        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws RuntimeException {
    }

    protected MatchObject createOnceMatchObject(List<GenericPrincipal.Attribute> grantAttributes, String className)
            throws Exception {
        if (SpaceTask.class.isAssignableFrom(ClassLoaderHelper.loadClass(className)))
            return new MatchObject(true);

        MatchObject result = null;
        ITypeDesc typeInfo = ((IInternalRemoteJSpaceAdmin) ((IDirectSpaceProxy)
                m_spaceProxy).getRemoteJSpace()).getClassDescriptor(className);

        if (typeInfo == null) {
            result = new MatchObject(new SpaceSecurityException(ITypeDesc.class.getSimpleName() + " is null for " + className));
        } else {
            /** Create map contains key - field name + field type; value - array index */
            Map<String, Integer> fieldIndexes = new HashMap<String, Integer>(typeInfo.getNumOfFixedProperties());
            for (int i = 0; i < typeInfo.getNumOfFixedProperties(); i++)
                fieldIndexes.put(typeInfo.getFixedProperty(i).getName(), i);

            RuntimeException fault = null;
            Map<Integer, List> grant = new HashMap<Integer, List>();
            for (Iterator<GenericPrincipal.Attribute> it = grantAttributes.iterator(); it.hasNext(); ) {
                GenericPrincipal.Attribute grantAttr = it.next();
                Integer index = fieldIndexes.get(grantAttr.name);
                if (index == null) {
                    fault = new SpaceSecurityException("no such " + grantAttr + " in class " + className);
                    break;
                }

                List values = grant.get(index);
                if (values == null) {
                    values = new ArrayList();
                    grant.put(index, values);
                }
                /**
                 * TODO Pay attention, if we have deal with java types that doesn't a part of
                 * java.lang package, we should take care of  MarshalledObject (depends on serialization type)
                 */
                Object value = ClassLoaderHelper.loadClass(typeInfo.getFixedProperty(index).getTypeName()).getConstructor(
                        String.class).newInstance(grantAttr.value);

                if (!values.contains(value))
                    values.add(value);

            }
            result = new MatchObject(Arrays.asList(typeInfo.getSuperClassesNames()), grant, fault);
        }
        return result;
    }


    protected boolean performMatching(MatchObject matchObj, Object[] content) {
        if (matchObj.getValue().length > 0 && content == null)
            return false;

        for (int i = 0; i < matchObj.getValue().length; i++)
            if (matchObj.getValue()[i] != null &&
                    !matchObj.getValue()[i].contains(content[matchObj.getIndex()[i]]))
                return false;

        return true;
    }

}