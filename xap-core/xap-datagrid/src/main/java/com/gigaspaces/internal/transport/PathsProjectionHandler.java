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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs paths projection on entry-packet data
 *
 * @author yechiel
 * @since 9.7
 */

@com.gigaspaces.api.InternalApi
public class PathsProjectionHandler {

    private final HashMap<String, PathsProjectionNode> _roots;
    private final List<Integer> _fixedProperties;
    private final List<String> _dynamicProperties;

    public PathsProjectionHandler(final String[] fixedPaths, final String[] dynamicPaths, ITypeDesc typeDesc) {

        List<Integer> fixedProperties = null;
        List<String> dynamicProperties = null;
        HashMap<String, List<PathProjectionInfo>> devidedPaths = new HashMap<String, List<PathProjectionInfo>>();
        if (fixedPaths != null) {
            fixedProperties = new ArrayList<Integer>(fixedPaths.length);
            for (int i = 0; i < fixedPaths.length; i++) {
                PathProjectionInfo pi = new PathProjectionInfo(fixedPaths[i]);
                List<PathProjectionInfo> cur = devidedPaths.get(pi.getTokens()[0]);
                if (cur == null) {
                    cur = new ArrayList<PathProjectionInfo>(fixedPaths.length);
                    devidedPaths.put(pi.getTokens()[0], cur);
                }
                cur.add(pi);
                fixedProperties.add(typeDesc.getFixedPropertyPosition(pi.getTokens()[0]));
            }
        }
        if (dynamicPaths != null) {
            dynamicProperties = new ArrayList<String>(dynamicPaths.length);
            for (String dynamicPath : dynamicPaths) {
                PathProjectionInfo pi = new PathProjectionInfo(dynamicPath);
                List<PathProjectionInfo> cur = devidedPaths.get(pi.getTokens()[0]);
                if (cur == null) {
                    cur = new ArrayList<PathProjectionInfo>(dynamicPaths.length);
                    devidedPaths.put(pi.getTokens()[0], cur);
                }
                cur.add(pi);
                dynamicProperties.add(pi.getTokens()[0]);
            }
        }

        //build roots map + projection tree
        HashMap<String, PathsProjectionNode> roots = new HashMap<String, PathsProjectionNode>();

        for (Map.Entry<String, List<PathProjectionInfo>> mapEntry : devidedPaths.entrySet()) {
            roots.put(mapEntry.getKey(), new PathsProjectionNode(mapEntry.getKey(), 0, mapEntry.getValue()));
        }
        _roots = roots;
        _fixedProperties = fixedProperties;
        _dynamicProperties = dynamicProperties;
    }


    public void applyFixedPathsProjections(IEntryPacket entryPacket, Object[] projectedValues) {
        for (int i : _fixedProperties) {
            String property = entryPacket.getTypeDescriptor().getFixedProperty(i).getName();
            Object newValue = buildProjectedPath(property, entryPacket.getFieldValue(i));
            projectedValues[i] = newValue;
        }
    }

    public void applyFixedPathsProjections(IEntryData entryData, Object[] projectedValues) {
        for (int i : _fixedProperties) {
            String property = entryData.getEntryTypeDesc().getTypeDesc().getFixedProperty(i).getName();
            Object newValue = buildProjectedPath(property, entryData.getFixedPropertiesValues()[i]);
            projectedValues[i] = newValue;
        }
    }

    public void applyDynamicPathsProjections(IEntryPacket entryPacket, Map<String, Object> projectedDynamicProperties) {
        for (String property : _dynamicProperties) {
            Object newValue = buildProjectedPath(property, entryPacket.getDynamicProperties().get(property));
            projectedDynamicProperties.put(property, newValue);
        }
    }


    //build an object using original value according to projection
    private Object buildProjectedPath(String property, Object value) {
        if (value == null)
            return null;

        PathsProjectionNode n = _roots.get(property);
        if (n == null)
            return null;

        return n.buildProjectedPath(value);
    }
}
