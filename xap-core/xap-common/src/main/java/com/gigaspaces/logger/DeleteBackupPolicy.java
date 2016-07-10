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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.LogManager;

/**
 * A backup policy that deletes any file which is older than the specified <tt>period</tt>, but
 * keeps at least as many of the specified <tt>backup</tt> files. <p> By default, a file is kept for
 * a 30 day <tt>period</tt>. After 30 days, the file is deleted; Unless if there are less than 10
 * <tt>backup</tt> files available. <p> These properties can be configured either by modifying the
 * logging configuration file or by use of a system property override, as specified by: <blockquote>
 * <code>-Dcom.gigaspaces.logger.DeleteBackupPolicy.[property-name]=[property-value]</code>
 * </blockquote> For example: <blockquote> <code>-Dcom.gigaspaces.logger.DeleteBackupPolicy.period=60</code></blockquote>
 * <p> <b>Note:</b> The {@link RollingFileHandler} is by default configured to use the {@link
 * NullBackupPolicy}.
 *
 * @author Moran Avigdor
 * @since 7.0
 */

public class DeleteBackupPolicy implements BackupPolicy {

    /**
     * Used to defined the prefix for system property overrides. <code>-Dcom.gigaspaces.logger.DeleteBackupPolicy.[property-name]=[property-value]</code>
     */
    private static final String SYSPROP_PROP_PREFIX = DeleteBackupPolicy.class.getName() + ".";
    private static final String PERIOD_PROPERTY = "period";
    private static final String BACKUP_PROPERTY = "backup";

    /*
     * Configuration Defaults
     */
    private static final int PERIOD_DEFAULT = 30;
    private static final int BACKUP_DEFAULT = 10;

    private static final long DAYS_TO_MILLIS = 86400000;
    private final List<File> files;
    private final long period;
    private final int backup;


    /**
     * Constructs a delete-backup policy specifying the <tt>period</tt> (in days) to keep a file
     * before deletion, and the number of files to keep as <tt>backup</tt>.
     */
    public DeleteBackupPolicy() {
        LogManager manager = LogManager.getLogManager();
        this.period = DAYS_TO_MILLIS * getPeriodProperty(resolvePolicyProperty(manager, PERIOD_PROPERTY));
        this.backup = getBackupProperty(resolvePolicyProperty(manager, BACKUP_PROPERTY));
        this.files = new ArrayList<File>(backup);
    }

    /**
     * Resolve logger property by its name - system property value if available, or logger property
     * extracted from the log configuration.
     *
     * @param manager  The log manger to extract logger property configuration from
     * @param property The property name (without a '.' prefix)
     * @return the value of the property, <code>null</code> if not available.
     */
    private String resolvePolicyProperty(LogManager manager, String property) {
        String propertyKey = SYSPROP_PROP_PREFIX + property;
        return System.getProperty(propertyKey, manager.getProperty(propertyKey));
    }

    /**
     * Convert the specified property value into an integer; If <code>null</code> use the default.
     *
     * @param property the period property.
     */
    private int getPeriodProperty(String property) {
        if (property == null) {
            return PERIOD_DEFAULT;
        }
        return Integer.valueOf(property).intValue();
    }

    /**
     * Convert the specified property value into an integer; If <code>null</code> use the default.
     *
     * @param property the backup property.
     */
    private int getBackupProperty(String property) {
        if (property == null) {
            return BACKUP_DEFAULT;
        }
        return Integer.valueOf(property).intValue();
    }

    /*
     * @see com.gigaspaces.logger.BackupPolicy#track(java.io.File)
     */
    public void track(File file) {
        if (!files.contains(file)) {
            files.add(file);
        }
        if (!exceededBackupsThreshold())
            return;

        final long today = System.currentTimeMillis();
        for (Iterator<File> iter = files.iterator(); exceededBackupsThreshold() && iter.hasNext(); ) {
            File f = iter.next();
            //track only files which are still relevant
            if (!f.exists()) {
                iter.remove();
                continue;
            }
            //delete only if the period has passed
            if (f.lastModified() + period > today) {
                break; //not yet
            }

            // delete file (don't care if failed as long as we stop tracking it)
            try {
                f.delete();
            } catch (Exception e) {
                System.err.println("Failed to delete file: " + f.getAbsolutePath() + " Caught: " + e);
                e.printStackTrace(System.err);
            }
            iter.remove();
        }
    }

    /**
     * Exceeds the backups threshold if the number of files we keep track of is larger than the
     * number of files we should be keeping as backup.
     *
     * @return <code>true</code> If exceeds; <code>false</code> otherwise.
     */
    private boolean exceededBackupsThreshold() {
        return files.size() > backup;
    }
}
