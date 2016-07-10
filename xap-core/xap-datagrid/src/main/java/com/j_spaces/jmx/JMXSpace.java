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

package com.j_spaces.jmx;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.JSpaceState;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.core.admin.StatisticsAdmin;
import com.j_spaces.core.admin.TemplateInfo;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.filters.StatisticsContext;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class JMXSpace extends XMLDescriptorsMBean {
    private IJSpace _remoteSpaceProxy;
    private Object m_spaceAdmin;
    public SpaceConfig m_spaceConfig;

    public JMXSpace() {
        super();
    }

    public JMXSpace(IJSpace remoteSpaceProxy, String xmlDescriptorsFileURL) throws Exception {
        super(xmlDescriptorsFileURL);
        _remoteSpaceProxy = remoteSpaceProxy;
        m_spaceAdmin = _remoteSpaceProxy.getAdmin();
        __setConfig(((IRemoteJSpaceAdmin) m_spaceAdmin).getConfig());
    }

    /**
     * Implements the abstract methods of AbstractDynamicMBean
     */
    @Override
    protected void __setConfig(Object config) {
        m_spaceConfig = (SpaceConfig) config;
    }

    @Override
    protected Object __getConfig() {
        return m_spaceConfig;
    }

    //************************************************
    //* Overriding of the getAttribute() and setAttribute() methods of parent class
    //************************************************

    /**
     * Allows the value of the specified attribute of the Dynamic MBean to be obtained.
     */
    public Object getAttribute(String attributeName)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attributeName == null || attributeName.trim().length() <= 0)
            throw new RuntimeOperationsException((new IllegalArgumentException
                    ("Attribute name can not be null or empty")),
                    "Unable to retrieve property from " + THIS_CLASS_NAME +
                            " with a null or empty attribute name string.");


        Object result =
                m_spaceConfig.getProperty(com.j_spaces.core.Constants.SPACE_CONFIG_PREFIX + attributeName);

        //try to receive value from parent class if such key does not exist
        if (result == null) {
            result = super.getAttribute(attributeName);
        }
        return result;
    }

    /**
     * Removes the entries that match the specified template from this space.
     */
    public void clear(String className)
            throws RemoteException, TransactionException, UnusableEntryException {
        _remoteSpaceProxy.clear(createTemplate(className), null);
    }

    /**
     * Returns the number of entries.
     */
    public int count(String className)
            throws RemoteException, TransactionException, UnusableEntryException {
        return _remoteSpaceProxy.count(createTemplate(className), null);
    }

    private static Object createTemplate(String typeName) {
        return typeName == null ? null : new SpaceDocument(typeName);
    }

    /**
     * Return the JavaSpace object
     */
    public IJSpace getSpace() {
        return _remoteSpaceProxy;
    }

    public String getSpaceName() {
        return m_spaceConfig.getSpaceName();
    }

    /**
     * Checks whether the space is alive and accessible.
     */
    public void ping() throws RemoteException {
        _remoteSpaceProxy.ping();
    }

    public Integer getState() throws RemoteException {
        return Integer.valueOf(((IRemoteJSpaceAdmin) m_spaceAdmin).getState());
    }

    public String getStateString() throws RemoteException {
        return JSpaceState.convertToString(getState());
    }

    public ClusterPolicy getClusterPolicy() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_spaceAdmin).getClusterPolicy();
    }

    public Object[] getReplicationStatus() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_spaceAdmin).getReplicationStatus();
    }

    public SpaceRuntimeInfo getRuntimeInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_spaceAdmin).getRuntimeInfo();
    }

    public List<IGSEntry> getTemplatesInfo(String className) throws RemoteException {
        ITypeDesc basicTypeInfo =
                ((IInternalRemoteJSpaceAdmin) m_spaceAdmin).getClassDescriptor(className);

        List<TemplateInfo> templatesInfoList =
                ((IInternalRemoteJSpaceAdmin) m_spaceAdmin).getTemplatesInfo(className);

        List<IGSEntry> resultList = new ArrayList<IGSEntry>(templatesInfoList.size());
        for (TemplateInfo templateInfo : templatesInfoList) {
            TemplateImpl templateImpl = new TemplateImpl(templateInfo, basicTypeInfo);
            resultList.add(templateImpl);
        }

        return resultList;
    }

    public void spaceCopy(String remoteUrl, Entry template, Boolean includeNotifyTemplatesObj, Integer chunkSizeObj)
            throws RemoteException {
        boolean includeNotifyTemplates =
                (includeNotifyTemplatesObj == null) ? false : includeNotifyTemplatesObj.booleanValue();
        int chunkSize = (chunkSizeObj == null) ? 1000 : chunkSizeObj.intValue();
        ((IRemoteJSpaceAdmin) m_spaceAdmin).spaceCopy(remoteUrl, template, includeNotifyTemplates, chunkSize);
    }

    public boolean isEmbedded() throws RemoteException {
        return _remoteSpaceProxy.isEmbedded();
    }

    public boolean isStatisticsAvailable() throws RemoteException {
        return ((StatisticsAdmin) m_spaceAdmin).isStatisticsAvailable();
    }

    public String[] getStatisticsStringArray() throws RemoteException {
        return ((StatisticsAdmin) m_spaceAdmin).getStatisticsStringArray();
    }

    public void setStatisticsSamplingRate(long rate) throws RemoteException {
        ((StatisticsAdmin) m_spaceAdmin).setStatisticsSamplingRate(rate);
    }

    public Long getStatisticsSamplingRate() throws RemoteException {
        return ((StatisticsAdmin) m_spaceAdmin).getStatisticsSamplingRate();
    }

    public StatisticsContext getStatistics(Integer operationCode) throws RemoteException {
        return ((StatisticsAdmin) m_spaceAdmin).getStatistics(operationCode);
    }

    public SpaceURL getURL() throws RemoteException {
        return _remoteSpaceProxy.getURL();
    }
}
