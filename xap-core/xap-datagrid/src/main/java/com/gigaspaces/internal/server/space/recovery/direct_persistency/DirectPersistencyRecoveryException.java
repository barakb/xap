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

//
package com.gigaspaces.internal.server.space.recovery.direct_persistency;

/**
 * Created by yechielf on 05/03/2015.
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyRecoveryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DirectPersistencyRecoveryException() {
        super();
    }

    public DirectPersistencyRecoveryException(String s) {
        super(s);
    }

    public DirectPersistencyRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public static DirectPersistencyRecoveryException createBackupNotFinishedRecoveryException(String spaceName) {
        return new DirectPersistencyRecoveryException("[" + spaceName + "] can't start space or elect space as primary because" +
                " this space has not yet finished to fully recover all data from the previous primary space");
    }

}
