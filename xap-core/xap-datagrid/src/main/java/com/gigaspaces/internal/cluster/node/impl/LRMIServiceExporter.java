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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.lrmi.GenericExporter;
import com.j_spaces.core.service.ServiceConfigLoader;

import java.rmi.Remote;
import java.rmi.server.ExportException;


@com.gigaspaces.api.InternalApi
public class LRMIServiceExporter implements IServiceExporter {
    private final GenericExporter _exporter;

    public LRMIServiceExporter() {
        _exporter = (GenericExporter) ServiceConfigLoader.getExporter();
    }

    public <T extends Remote> T export(T service) throws ExportException {
        return (T) _exporter.export(service);
    }

    public <T extends Remote> void unexport(T service) {
        _exporter.unexport(service);
    }

}
