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

package com.gigaspaces.internal.remoting.routing;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.logger.Constants;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class AbstractRemoteOperationRouter implements RemoteOperationRouter {
    protected final Logger _logger;

    public AbstractRemoteOperationRouter(String name) {
        this._logger = Logger.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER + '.' + name);
    }

    protected void logBeforeExecute(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, boolean oneway) {
        if (oneway)
            logBeforeExecuteOneway(proxy, request);
        else if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Starting execution of " + proxy.toLogMessage(request));
    }

    protected void logBeforeExecuteAsync(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Starting async execution of " + proxy.toLogMessage(request));
    }

    private void logBeforeExecuteOneway(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Starting oneway execution of " + proxy.toLogMessage(request));
    }


    protected void logAfterExecute(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, RemoteOperationResult result, boolean oneway) {
        if (oneway)
            logAfterExecuteOneway(proxy, request);
        else if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Execution result: " + result + ", request=" + proxy.toLogMessage(request));
    }

    protected void logAfterExecuteAsync(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, RemoteOperationResult result) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Async execution result: " + result + ", request=" + proxy.toLogMessage(request));
    }

    private void logAfterExecuteOneway(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, "Oneway execution completed, request=" + proxy.toLogMessage(request));
    }

    protected void logExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception, boolean oneway) {
        if (oneway)
            logOnewayExecutionFailure(proxy, request, exception);
        else if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logAsyncExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Async execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    private void logOnewayExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Oneway execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logInterruptedExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception, boolean oneway) {
        if (oneway)
            logInterruptedOnewayExecution(proxy, request, exception);
        else if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logInterruptedAsyncExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Async execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logUnexpectedAsyncExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Async execution failed unexpectedly: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    private void logInterruptedOnewayExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Oneway execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    @Override
    public RemoteOperationsExecutorProxy getCachedMember() {
        throw new UnsupportedOperationException();
    }
}
