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


package com.gigaspaces.logger;

import java.io.File;

/**
 * A null backup policy which acts as a placeholder for a 'do-nothing' behavior. Used when an
 * exception occurs trying to instantiate a user defined backup policy, or if no policy is desired.
 *
 * @author Moran Avigdor
 * @see RollingFileHandler
 * @since 7.0
 */

public class NullBackupPolicy implements BackupPolicy {

    /*
     * @see com.gigaspaces.logger.BackupPolicy#track(java.io.File)
     */
    public void track(File file) {
    }

}
