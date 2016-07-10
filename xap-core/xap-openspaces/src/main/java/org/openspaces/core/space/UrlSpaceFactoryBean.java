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
import com.gigaspaces.cluster.activeelection.LeaderSelectorConfig;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.IJSpace;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.config.AttributeStoreFactoryBean;
import org.openspaces.core.config.BlobStoreDataPolicyFactoryBean;
import org.openspaces.core.config.CustomCachePolicyFactoryBean;
import org.openspaces.core.config.SpaceSqlFunctionBean;
import org.openspaces.core.extension.SpaceCustomComponentFactoryBean;
import org.openspaces.core.properties.BeanLevelMergedPropertiesAware;
import org.openspaces.core.space.filter.FilterProviderFactory;
import org.openspaces.core.space.filter.replication.ReplicationFilterProviderFactory;
import org.openspaces.core.transaction.DistributedTransactionProcessingConfigurationFactoryBean;
import org.springframework.dao.DataAccessException;

import java.util.Map;
import java.util.Properties;

/**
 * A space factory bean that creates a space ({@link IJSpace}) based on a url.
 *
 * <p>The factory allows to specify url properties using {@link #setUrlProperties(java.util.Properties)
 * urlProperties} and space parameters using {@link #setParameters(java.util.Map) parameters} or
 * using {@link #setProperties(Properties) properties}. It also accepts a {@link ClusterInfo} using
 * {@link #setClusterInfo(ClusterInfo)} and translates it into the relevant space url properties
 * automatically.
 *
 * <p>Most url properties are explicitly exposed using different setters. Though they can also be
 * set using the {@link #setUrlProperties(java.util.Properties) urlProperties} the explicit setters
 * allow for more readable and simpler configuration. Some examples of explicit url properties are:
 * {@link #setSchema(String)}, {@link #setFifo(boolean)}.
 *
 * <p>The factory uses the {@link BeanLevelMergedPropertiesAware} in order to be injected with
 * properties that were not parameterized in advance (using ${...} notation). This will directly
 * inject additional properties in the Space creation/finding process.
 *
 * @author kimchy
 */
public class UrlSpaceFactoryBean extends AbstractSpaceFactoryBean implements BeanLevelMergedPropertiesAware, ClusterInfoAware {

    private final InternalSpaceFactory factory = new InternalSpaceFactory();
    private String url;

    /**
     * Creates a new url space factory bean. The url parameters is requires so the {@link
     * #setUrl(String)} must be called before the bean is initialized.
     */
    public UrlSpaceFactoryBean() {
    }

    /**
     * Creates a new url space factory bean based on the url provided.
     *
     * @param url The url to create the {@link com.j_spaces.core.IJSpace} with.
     */
    public UrlSpaceFactoryBean(String url) {
        this.url = url;
    }

    /**
     * Creates a new url space factory bean based on the url and map parameters provided.
     *
     * @param url    The url to create the {@link IJSpace} with.
     * @param params The parameters to create the {@link IJSpace} with.
     */
    public UrlSpaceFactoryBean(String url, Map<String, Object> params) {
        this(url);
        setParameters(params);
    }

    /**
     * Creates the space.
     */
    @Override
    protected IJSpace doCreateSpace() throws DataAccessException {
        return factory.create(this, url);
    }

    /**
     * Sets the space as secured. Note, when passing userName and password it will automatically be
     * secured.
     */
    public void setSecured(boolean secured) {
        factory.getFactory().setSecured(secured);
    }

    @Override
    public void setSecurityConfig(SecurityConfig securityConfig) {
        super.setSecurityConfig(securityConfig);
        factory.setSecurityConfig(securityConfig);
    }

    /**
     * Sets the url the {@link IJSpace} will be created with. Note this url does not take affect
     * after the bean has been initialized.
     *
     * @param url The url to create the {@link IJSpace} with.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    void setInstanceId(String instanceId) {
        factory.getFactory().setInstanceId(instanceId);
    }

    /**
     * Sets the parameters the {@link IJSpace} will be created with. Note this parameters does not
     * take affect after the bean has been initialized.
     *
     * <p> Note, this should not be confused with {@link #setUrlProperties(java.util.Properties)}.
     * The parameters here are the ones referred to as custom properties and allows for example to
     * control the xpath injection to space schema.
     *
     * @param parameters The parameters to create the {@link com.j_spaces.core.IJSpace} with.
     */
    public void setParameters(Map<String, Object> parameters) {
        factory.getFactory().setParameters(parameters);
    }

    /**
     * Same as {@link #setParameters(java.util.Map) parameters} just with properties for simpler
     * configuration.
     */
    public void setProperties(Properties properties) {
        factory.getFactory().setProperties(properties);
    }

    /**
     * Sets the url properties. Note, most if not all url level properties can be set using explicit
     * setters.
     */
    public void setUrlProperties(Properties urlProperties) {
        factory.getFactory().setUrlProperties(urlProperties);
    }

    /**
     * The space instance is created using a space schema file which can be used as a template
     * configuration file for creating a space. The user specifies one of the pre-configured schema
     * names (to create a space instance from its template) or a custom one using this property.
     *
     * <p>If a schema name is not defined, a default schema name called <code>default</code> will be
     * used.
     */
    public void setSchema(String schema) {
        factory.getFactory().setSchema(schema);
    }

    /**
     * Indicates that all take/write operations be conducted in FIFO mode. Default is the Space
     * default (<code>false</code>).
     */
    public void setFifo(boolean fifo) {
        factory.getFactory().setFifo(fifo);
    }

    /**
     * The Jini Lookup Service group to find container or space using multicast (jini protocol).
     * Groups are comma separated list.
     */
    public void setLookupGroups(String lookupGroups) {
        factory.getFactory().setLookupGroups(lookupGroups);
    }

    /**
     * The Jini Lookup locators for the Space. In the form of: <code>host1:port1,host2:port2</code>.
     */
    public void setLookupLocators(String lookupLocators) {
        factory.getFactory().setLookupLocators(lookupLocators);
    }

    /**
     * The max timeout in <b>milliseconds</b> to find a Container or Space using multicast (jini
     * protocol). Defaults to <code>6000</code> (i.e. 6 seconds).
     */
    public void setLookupTimeout(Integer lookupTimeout) {
        factory.getFactory().setLookupTimeout(lookupTimeout);
    }

    /**
     * When <code>false</code>, optimistic lock is disabled. Default to the Space default value.
     */
    public void setVersioned(boolean versioned) {
        factory.getFactory().setVersioned(versioned);
    }

    /**
     * If <code>true</code> - Lease object would not return from the write/writeMultiple operations.
     * Defaults to the Space default value (<code>false</code>).
     */
    public void setNoWriteLease(boolean noWriteLease) {
        // Ignore - NoWriteLease is no longer supported.
    }

    /**
     * When setting this URL property to <code>true</code> it will allow the space to connect to the
     * Mirror service to push its data and operations for asynchronous persistency. Defaults to the
     * Space default (which defaults to <code>false</code>).
     */
    public void setMirror(boolean mirror) {
        factory.getFactory().setMirror(mirror);
    }

    /**
     * Inject a list of filter provider factories providing the ability to inject actual Space
     * filters.
     */
    public void setFilterProviders(FilterProviderFactory[] filterProviders) {
        factory.setFilterProviders(filterProviders);
    }

    /**
     * Injects a replication provider allowing to directly inject actual replication filters.
     */
    public void setReplicationFilterProvider(ReplicationFilterProviderFactory replicationFilterProvider) {
        factory.setReplicationFilterProvider(replicationFilterProvider);
    }

    /**
     * A data source
     */
    public void setExternalDataSource(ManagedDataSource externalDataSource) {
        factory.getFactory().setExternalDataSource(externalDataSource);
    }

    /**
     * Sets the {@link SpaceDataSource} which will be used as a data source for the space.
     *
     * @param spaceDataSource The {@link SpaceDataSource} instance.
     */
    public void setSpaceDataSource(SpaceDataSource spaceDataSource) {
        factory.getFactory().setSpaceDataSource(spaceDataSource);
    }

    /**
     * @param spaceSynchronizationEndpoint
     */
    public void setSpaceSynchronizationEndpoint(SpaceSynchronizationEndpoint spaceSynchronizationEndpoint) {
        factory.getFactory().setSpaceSynchronizationEndpoint(spaceSynchronizationEndpoint);
    }

    /**
     * Inject a list of space types.
     */
    public void setSpaceTypes(SpaceTypeDescriptor[] typeDescriptors) {
        factory.getFactory().setTypeDescriptors(typeDescriptors);
    }

    /**
     * Inject a list of space types.
     */
    public void setQueryExtensionProviders(QueryExtensionProvider[] queryExtenstionProviders) {
        factory.getFactory().setQueryExtensionProviders(queryExtenstionProviders);
    }

    /**
     * Sets the cache policy that the space will use. If not set, will default to the one configured
     * in the space schema.
     *
     * @see org.openspaces.core.space.AllInCachePolicy
     * @see org.openspaces.core.space.LruCachePolicy
     * @see org.openspaces.core.space.CustomCachePolicy
     * @see org.openspaces.core.space.BlobStoreDataCachePolicy
     */
    public void setCachePolicy(CachePolicy cachePolicy) {
        factory.setCachePolicy(cachePolicy);
    }

    /**
     * Externally managed override properties using open spaces extended config support. Should not
     * be set directly but allowed for different Spring context container to set it.
     */
    public void setMergedBeanLevelProperties(Properties beanLevelProperties) {
        factory.getFactory().setBeanLevelProperties(beanLevelProperties);
    }

    public void attributeStore(AttributeStore attributeStore) {
        factory.setAttributeStore(attributeStore);
    }

    public void leaderSelector(LeaderSelectorConfig leaderSelectorConfig) {
        factory.setLeaderSelectorConfig(leaderSelectorConfig);
    }

    /**
     * Injected thanks to this bean implementing {@link ClusterInfoAware}. If set will use the
     * cluster information in order to configure the url based on it.
     */
    public void setClusterInfo(ClusterInfo clusterInfo) {
        factory.setClusterInfo(clusterInfo);
    }

    /**
     * Sets the gateway replication targets to be used with the constructed space.
     *
     * @param gatewayFactoryBean The gateway targets factory bean.
     * @deprecated Since 12.0 - Use #setCustomComponent instead.
     */
    @Deprecated
    public void setGatewayTargets(SpaceCustomComponentFactoryBean gatewayFactoryBean) {
        setCustomComponent(gatewayFactoryBean);
    }

    public void setCustomComponent(SpaceCustomComponentFactoryBean customComponentFactoryBean) {
        if (customComponentFactoryBean != null)
            factory.addCustomComponent(customComponentFactoryBean.createSpaceComponent());
    }

    /**
     * Sets the distributed transaction processing configuration for the Mirror component.
     *
     * @param distributedTransactionProcessingConfiguration The distributed transaction processing
     *                                                      configuration to set.
     */
    public void setDistributedTransactionProcessingConfiguration(
            DistributedTransactionProcessingConfigurationFactoryBean distributedTransactionProcessingConfiguration) {
        factory.setDistributedTransactionProcessingConfiguration(distributedTransactionProcessingConfiguration);
    }

    public void setCustomCachePolicy(CustomCachePolicyFactoryBean customCachePolicy) {
        if (customCachePolicy != null)
            setCachePolicy(customCachePolicy.asCachePolicy());
    }

    public void setBlobStoreDataPolicy(BlobStoreDataPolicyFactoryBean blobStoreDataPolicy) {
        if (blobStoreDataPolicy != null)
            setCachePolicy(blobStoreDataPolicy.asCachePolicy());
    }

    public void setAttributeStore(AttributeStoreFactoryBean attributeStore) {
        if (attributeStore != null)
            factory.setAttributeStore(attributeStore.getStoreHandler());
    }

    public void setLeaderSelectorConfig(LeaderSelectorFactoryBean leaderSelectorFactoryBean) {
        if (leaderSelectorFactoryBean != null)
            factory.setLeaderSelectorConfig(leaderSelectorFactoryBean.getConfig());
    }

    public void setSpaceSqlFunction(SpaceSqlFunctionBean[] spaceSqlFunctionBeans) {
        factory.setSpaceSqlFunction(spaceSqlFunctionBeans);
    }
}
