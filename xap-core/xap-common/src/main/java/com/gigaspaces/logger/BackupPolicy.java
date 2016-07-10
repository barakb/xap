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
import java.util.logging.LogManager;

/**
 * An interface for a pluggable backup policy. Implementation may wish to zip files if reached a
 * certain threshold. By default a {@link NullBackupPolicy} is used, but can be replaced by a {@link
 * DeleteBackupPolicy} to keep a backup of files, but delete old ones. <p> Implementations can
 * derive their configuration properties from the {@link LogManager} directly. <code>
 * <pre>
 * For example, retrieve a <tt>threshold</tt> property:<p>
 * LogManager manager = LogManager.getLogManager();
 * String cname = this.getClass.getName();
 * Integer threshold = Integer.valueOf(manager.getProperty(cname+".threshold"));
 * ...
 * </pre>
 * </code>
 *
 * @author Moran Avigdor
 * @see NullBackupPolicy
 * @see DeleteBackupPolicy
 * @see RollingFileHandler
 * @since 7.0
 */
public interface BackupPolicy {
    /**
     * Track a newly created file. A file is either created upon rollover or at initialization time.
     * Implementation can keep track of files and decide whether to trigger the backup policy.
     *
     * @param file A newly created file.
     */
    public void track(File file);
}
