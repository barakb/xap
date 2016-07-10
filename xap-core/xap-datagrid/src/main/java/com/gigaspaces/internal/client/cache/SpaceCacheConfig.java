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

package com.gigaspaces.internal.client.cache;

import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.Constants.DCache;
import com.j_spaces.core.client.SpaceURL;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
public abstract class SpaceCacheConfig {
    public final static String FROM_DOTNET = SpaceUrlUtils.toCustomUrlProperty("fromDotNet");

    public static final long DEFAULT_MAX_DISCONNECTION_DURATION = 60000l;
    private final Logger _logger;

    private Properties _customProperties;
    private SpaceURL _remoteSpaceUrl;
    private Long _maxDisconnectionDuration;
    private Integer _batchSize;
    private Long _batchTimeout;
    private String _storageType;

    private Long _notifyRenewRTT;
    private EntryDataType _entryDataType;

    public SpaceCacheConfig() {
        this(null, new Properties());
    }

    public SpaceCacheConfig(SpaceURL remoteSpaceUrl, Properties customProperties) {
        _logger = initLogger();
        this._remoteSpaceUrl = remoteSpaceUrl;
        this._customProperties = customProperties;
        this._entryDataType = EntryDataType.USER_TYPE;
    }

    protected abstract Logger initLogger();

    public void initLocalSpaceConfig(SpaceConfigReader configReader) {
        if (_storageType != null)
            configReader.setSpaceProperty(DCache.STORAGE_TYPE_PROP, _storageType);
    }

    public Properties getCustomProperties() {
        return _customProperties;
    }

    public void setCustomProperties(Properties customProperties) {
        if (customProperties != null)
            this._customProperties = customProperties;
        else
            this._customProperties.clear();
    }

    public SpaceURL getRemoteSpaceUrl() {
        return _remoteSpaceUrl;
    }

    public void initRemoteSpaceUrl(IDirectSpaceProxy spaceProxy) {
        if (_remoteSpaceUrl == null) {
            _remoteSpaceUrl = (SpaceURL) spaceProxy.getFinderURL().clone();
            _remoteSpaceUrl.putAll(_customProperties);
            _remoteSpaceUrl.getCustomProperties().putAll(_customProperties);
        }

        final Properties customProperties = _remoteSpaceUrl.getCustomProperties();

        // LocalCache
        _remoteSpaceUrl.setProperty(SpaceURL.CREATE, Boolean.FALSE.toString());
        _remoteSpaceUrl.remove(SpaceURL.USE_LOCAL_CACHE);
        if (customProperties != null)
            customProperties.remove(SpaceURL.USE_LOCAL_CACHE);

        // Security
        _remoteSpaceUrl.remove(SpaceURL.SECURED);
        if (customProperties != null)
            customProperties.remove(SpaceURL.SECURED);

        applyDefaultValues();
    }

    protected void applyDefaultValues() {
        if (_maxDisconnectionDuration == null) {
            String value = getDeprecatedProperty("space-config.dist-cache.events.lease", "max-disconnection-duration");
            if (!StringUtils.hasLength(value))
                value = getDeprecatedProperty("space-config.dist-cache.events.lease-renew.duration", "max-disconnection-duration");
            _maxDisconnectionDuration = StringUtils.hasLength(value) ? Long.parseLong(value) : DEFAULT_MAX_DISCONNECTION_DURATION;
        }
        if (_batchSize == null) {
            String value = getDeprecatedProperty("space-config.dist-cache.events.batch.size", "batch-size");
            _batchSize = StringUtils.hasLength(value) ? Integer.parseInt(value) : 1000;
        }
        if (_batchTimeout == null) {
            String value = getDeprecatedProperty("space-config.dist-cache.events.batch.timeout", "batch-timeout");
            _batchTimeout = StringUtils.hasLength(value) ? Long.parseLong(value) : 100l;
        }

        if (_notifyRenewRTT == null) {
            String value = getUrlProperty("space-config.dist-cache.events.lease-renew.round-trip-time");
            _notifyRenewRTT = StringUtils.hasLength(value) ? Long.parseLong(value) : EventSessionConfig.DEFAULT_RENEW_RTT;
        }

        if (_storageType == null) {
            _storageType = getDeprecatedProperty(SpaceURL.LOCAL_CACHE_STORAGE_TYPE, null);
            // If storage type was not set explicitly and this is from .NET, set default for .NET local cache:
            if (_storageType == null && Boolean.parseBoolean(getUrlProperty(SpaceCacheConfig.FROM_DOTNET)))
                setStorageType(SpaceURL.LOCAL_CACHE_STORE_SHALLOW_COPY);
        }
    }

    protected String getUrlProperty(String key) {
        String result = _remoteSpaceUrl.getProperty(key);
        if (!StringUtils.hasLength(result))
            result = _remoteSpaceUrl.getCustomProperties().getProperty(key);
        return result;
    }

    protected String getDeprecatedProperty(String key, String alternateProperty) {
        String result = getUrlProperty(key);

        if (StringUtils.hasLength(result) && _logger.isLoggable(Level.WARNING)) {
            String message = "Property '" + key + "' is deprecated";
            if (alternateProperty == null)
                message += ".";
            else
                message += " - use '" + alternateProperty + "' with OpenSpaces configurers instead.";

            _logger.log(Level.WARNING, message);
        }

        return result;
    }

    public void setStorageType(String storageType) {
        _storageType = storageType;
        _entryDataType = storageType.equalsIgnoreCase(SpaceURL.LOCAL_CACHE_STORE_REFERENCE)
                ? EntryDataType.USER_TYPE : EntryDataType.FLAT;
    }

    public Long getMaxDisconnectionDuration() {
        return _maxDisconnectionDuration;
    }

    public void setMaxDisconnectionDuration(Long maxDisconnectionDuration) {
        this._maxDisconnectionDuration = maxDisconnectionDuration;
    }

    public Integer getBatchSize() {
        return _batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this._batchSize = batchSize;
    }

    public Long getBatchTimeout() {
        return _batchTimeout;
    }

    public void setBatchTimeout(Long batchTimeout) {
        this._batchTimeout = batchTimeout;
    }

    public long getNotifyRenewRTT() {
        return _notifyRenewRTT;
    }

    public abstract boolean isActiveWhileDisconnected();

    public abstract boolean supportsNotifications();

    public EntryDataType getEntryDataType() {
        return _entryDataType;
    }

}
