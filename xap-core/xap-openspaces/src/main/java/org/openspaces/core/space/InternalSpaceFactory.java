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

package org.openspaces.core.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.client.ClusterConfig;
import com.gigaspaces.client.SpaceProxyFactory;
import com.gigaspaces.cluster.activeelection.LeaderSelectorConfig;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.sync.mirror.MirrorDistributedTxnConfig;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.server.SpaceCustomComponent;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.FilterProvider;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.config.SpaceSqlFunctionBean;
import org.openspaces.core.space.filter.FilterProviderFactory;
import org.openspaces.core.space.filter.replication.ReplicationFilterProviderFactory;
import org.openspaces.core.transaction.DistributedTransactionProcessingConfigurationFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

public class InternalSpaceFactory {
    private static final boolean enableExecutorInjection = true;
    private final SpaceProxyFactory factory = new SpaceProxyFactory();
    private ClusterInfo clusterInfo;

    public SpaceProxyFactory getFactory() {
        return factory;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public void setCachePolicy(CachePolicy cachePolicy) {
        factory.setCachePolicyProperties(cachePolicy == null ? null : cachePolicy.toProps());
    }

    public void addCustomComponent(SpaceCustomComponent component) {
        factory.addCustomComponent(component);
    }

    public void setDistributedTransactionProcessingConfiguration(DistributedTransactionProcessingConfigurationFactoryBean distributedTransactionProcessingConfiguration) {
        MirrorDistributedTxnConfig mirrorDistributedTxnConfig = null;
        if (distributedTransactionProcessingConfiguration != null) {
            mirrorDistributedTxnConfig = new MirrorDistributedTxnConfig()
                    .setDistributedTransactionWaitForOperations(distributedTransactionProcessingConfiguration.getDistributedTransactionWaitForOperations())
                    .setDistributedTransactionWaitTimeout(distributedTransactionProcessingConfiguration.getDistributedTransactionWaitTimeout());
        }
        factory.setMirrorDistributedTxnConfig(mirrorDistributedTxnConfig);
    }

    public void setFilterProviders(FilterProviderFactory[] filterProviders) {
        FilterProvider[] spaceFilterProviders = null;
        if (filterProviders != null) {
            spaceFilterProviders = new FilterProvider[filterProviders.length];
            for (int i = 0; i < filterProviders.length; i++) {
                spaceFilterProviders[i] = filterProviders[i].getFilterProvider();
            }
        }
        factory.setFilterProviders(spaceFilterProviders);
    }

    public void setReplicationFilterProvider(ReplicationFilterProviderFactory replicationFilterProvider) {
        factory.setReplicationFilterProvider(replicationFilterProvider == null ? null : replicationFilterProvider.getFilterProvider());
    }

    public void setSecurityConfig(SecurityConfig securityConfig) {
        factory.setCredentialsProvider(securityConfig == null ? null : securityConfig.getCredentialsProvider());
    }

    public void setAttributeStore(AttributeStore attributeStore) {
        factory.setAttributeStore(attributeStore);
    }

    public void setLeaderSelectorConfig(LeaderSelectorConfig leaderSelectorConfig) {
        factory.setLeaderSelectorConfig(leaderSelectorConfig);
    }

    public IJSpace create(AbstractSpaceFactoryBean spaceFactoryBean, String url) {
        Assert.notNull(url, "url property is required");
        boolean isRemote = SpaceUrlUtils.isRemoteProtocol(url);
        factory.setClusterConfig(toClusterConfig(url, clusterInfo));
        beforeCreateSpace(spaceFactoryBean, isRemote);
        try {
            return factory.createSpaceProxy(url);
        } catch (MalformedURLException e) {
            throw new CannotCreateSpaceException("Failed to parse url [" + url + "]", e);
        } catch (FinderException e) {
            if (isRemote) {
                throw new CannotFindSpaceException("Failed to find space with url " + url + "", e);
            }
            throw new CannotCreateSpaceException("Failed to create space with url " + url + "", e);
        }
    }

    public IJSpace create(AbstractSpaceFactoryBean spaceFactoryBean, String name, boolean isRemote) {
        if (name == null)
            name = spaceFactoryBean.getName();
        Assert.notNull(name, "name property is required");
        factory.setClusterConfig(toClusterConfig(clusterInfo));
        beforeCreateSpace(spaceFactoryBean, isRemote);
        try {
            return factory.createSpaceProxy(name, isRemote);
        } catch (MalformedURLException e) {
            throw new CannotCreateSpaceException("Failed to build url for space [" + name + "]", e);
        } catch (FinderException e) {
            if (isRemote) {
                throw new CannotFindSpaceException("Failed to find space " + name + "", e);
            }
            throw new CannotCreateSpaceException("Failed to create space " + name + "", e);
        }
    }

    private static ClusterConfig toClusterConfig(String url, ClusterInfo clusterInfo) {
        if (clusterInfo == null || SpaceUrlUtils.isRemoteProtocol(url))
            return null;

        if (url.indexOf(SpaceURL.CLUSTER_SCHEMA + "=") == -1 && !StringUtils.hasText(clusterInfo.getSchema()))
            return null;
        ClusterConfig clusterConfig = new ClusterConfig();
        if (url.indexOf(SpaceURL.CLUSTER_SCHEMA + "=") == -1)
            clusterConfig.setSchema(clusterInfo.getSchema());
        if (url.indexOf("&" + SpaceURL.CLUSTER_TOTAL_MEMBERS + "=") == -1 && url.indexOf("?" + SpaceURL.CLUSTER_TOTAL_MEMBERS + "=") == -1) {
            clusterConfig.setNumberOfInstances(clusterInfo.getNumberOfInstances());
            clusterConfig.setNumberOfBackups(clusterInfo.getNumberOfBackups());
        }
        if (url.indexOf("&" + SpaceURL.CLUSTER_MEMBER_ID + "=") == -1 && url.indexOf("?" + SpaceURL.CLUSTER_MEMBER_ID + "=") == -1)
            clusterConfig.setInstanceId(clusterInfo.getInstanceId());

        if (url.indexOf("&" + SpaceURL.CLUSTER_BACKUP_ID + "=") == -1 && url.indexOf("?" + SpaceURL.CLUSTER_BACKUP_ID + "=") == -1)
            clusterConfig.setBackupId(clusterInfo.getBackupId());

        return clusterConfig;
    }

    private static ClusterConfig toClusterConfig(ClusterInfo clusterInfo) {
        if (clusterInfo == null)
            return null;
        if (!StringUtils.hasText(clusterInfo.getSchema()))
            return null;
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setSchema(clusterInfo.getSchema());
        clusterConfig.setNumberOfInstances(clusterInfo.getNumberOfInstances());
        clusterConfig.setNumberOfBackups(clusterInfo.getNumberOfBackups());
        clusterConfig.setInstanceId(clusterInfo.getInstanceId());
        clusterConfig.setBackupId(clusterInfo.getBackupId());
        return clusterConfig;
    }

    private void beforeCreateSpace(AbstractSpaceFactoryBean spaceFactoryBean, boolean isRemote) {
        if (!isRemote && enableExecutorInjection) {
            FilterProvider filterProvider = new FilterProvider("InjectionExecutorFilter", new ExecutorSpaceFilter(spaceFactoryBean, clusterInfo));
            filterProvider.setOpCodes(FilterOperationCodes.BEFORE_EXECUTE);
            factory.addFilterProvider(filterProvider);
        }
    }

    public void setSpaceSqlFunction(SpaceSqlFunctionBean[] spaceSqlFunctionBeans) {
        Map<String, SqlFunction> map = new HashMap<String, SqlFunction>();
        for (SpaceSqlFunctionBean spaceSqlFunctionBean : spaceSqlFunctionBeans) {
            String functionName = spaceSqlFunctionBean.getFunctionName();
            SqlFunction sqlFunction = null;
            try {
                sqlFunction = (SqlFunction) spaceSqlFunctionBean.getSqlFunction();
            } catch (ClassCastException e) {
                throw new RuntimeException("provided class is not a SqlFunction type");
            }
            if (sqlFunction != null) {
                map.put(functionName, sqlFunction);
            }
        }
        factory.setUserDefineSqlFunctions(map);
    }
}