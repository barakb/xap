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

/*
 * @(#)LRMIStubHandlerImpl.java 1.0 08/10/2002 17:57PM
 */

package com.gigaspaces.internal.lrmi.stubs;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.lrmi.GenericExporter;
import com.j_spaces.core.IStubHandler;
import com.j_spaces.core.service.ServiceConfigLoader;

import net.jini.export.Exporter;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Provides a pluggable communication adaptor, LRMI (Light Remote Method Invocation). The adaptor
 * manages the client communication with the client/server. The default adaptor supports two
 * communication protocols, RMI (default protocol) and NIO. RMI and NIO are the optional
 * communication protocols between the space proxy and the space server.
 *
 * @author Igor Goldenberg
 * @version 2.5
 * @deprecated Use {@link Exporter} instead or {@link GenericExporter}
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class LRMIStubHandlerImpl implements IStubHandler {
    private static final long serialVersionUID = 2L;

    // keep all non-generic exporters if exists
    private transient Map<Remote, Exporter> _expotersTable;
    // generic exporter and configuration if exists
    private transient GenericExporter _genericExporter;
    private ITransportConfig _transportConfig;

    public LRMIStubHandlerImpl() {
        _init();
        if (_genericExporter != null)
            _transportConfig = _genericExporter.getConfiguration();
    }

    @Override
    public Remote exportObject(Remote obj) throws ExportException {
        if (_genericExporter != null)
            return _genericExporter.export(obj);

        /* we got here if it's not generic exporter path, keep mapping remote-obj == exporter */
        synchronized (_expotersTable) {
            Exporter exporter = _expotersTable.get(obj);
            if (exporter == null) {
                exporter = ServiceConfigLoader.getExporter();
                _expotersTable.put(obj, exporter);
            }

            return exporter.export(obj);
        }
    }

    @Override
    public void unexportObject(Remote obj) {
        if (_genericExporter != null)
            _genericExporter.unexport(obj);
        else {
            Exporter exporter = _expotersTable.remove(obj);
            if (exporter != null)
                exporter.unexport(true);
        }
    }

    @Override
    public ITransportConfig getTransportConfig() {
        return _transportConfig;
    }

    private void _init() {
        _expotersTable = Collections.synchronizedMap(new WeakHashMap<Remote, Exporter>());
        Exporter exporter = ServiceConfigLoader.getExporter();

        if (exporter instanceof GenericExporter)
            _genericExporter = (GenericExporter) exporter;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // reinit on de-serialization
        _init();
    }
}
