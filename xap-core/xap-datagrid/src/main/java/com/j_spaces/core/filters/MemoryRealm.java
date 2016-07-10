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

package com.j_spaces.core.filters;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.SecurityContext;
import com.j_spaces.core.SpaceSecurityException;
import com.j_spaces.kernel.DesEncrypter;

import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class reads an XML file to configure the valid users, passwords, and roles.  The file format
 * (and default file location) are identical to those currently supported by Tomcat 3.X. <p>
 * IMPLEMENTATION NOTE: It is assumed that the in-memory collection representing our defined users
 * (and their roles) is initialized at application startup and never modified again. ( ONLY ON CLEAN
 * SPACE ).
 *
 * @author Igor Goldenberg
 * @version 1.2
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class MemoryRealm
        implements ISpaceUserAccountDriver {
    private static DesEncrypter m_encrypter = DesEncrypter.getInstance();

    /**
     * class contains all information about space filter configuration
     */
    private FiltersInfo filterInfo = null;

    /**
     * contains the user information: userName, password and roles.
     */
    public final Hashtable<String, GenericPrincipal> m_userPrincipal = new Hashtable<String, GenericPrincipal>();
    /**
     * User defined roles contains as key: role name, value: UserDefinedRole object.
     */
    public final Map<String, UserDefinedRole> m_userDefinedRoles = new Hashtable<String, UserDefinedRole>();
    //logger
    final private static Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_FILTERS);


    public MemoryRealm() {
    }

    /**
     * Re/initialize user account driver by reading specified data file. This file contains an
     * encrypted and serialized <code>FiltersInfo</code> object.
     */
    public void init(String URItoPolicyFile, String containerName)
            throws IOException, SAXException, Exception {
        try {
            FileInputStream fis = new FileInputStream(URItoPolicyFile);
            byte[] byteArray = new byte[fis.available()];
            fis.read(byteArray);
            byteArray = m_encrypter.dencrypt(byteArray);
            filterInfo = (FiltersInfo) IOUtils.objectFromByteBuffer(byteArray);
            fis.close();

            if (_logger.isLoggable(Level.CONFIG)) {
                _logger.log(Level.CONFIG, "Loaded users security permissions file from <" + URItoPolicyFile + ">");
            }
        } catch (FileNotFoundException ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to load users security permissions file: " + ex.getMessage());
            }
        } catch (IOException ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to load users security permissions file: " + ex.getMessage());
            }
        }
        init(containerName);
    }

    /**
     * Re/initialize user account driver by processing a new <code>FiltersInfo</code> object. In
     * case of <code>FiltersInfo</code> equals null, only container administrator will be
     * initialized.
     */
    private void init(String containerName) {
        if (filterInfo != null) {
            if (filterInfo.userDefinedRoles != null) {
                for (int i = 0; i < filterInfo.userDefinedRoles.length; i++) {
                    m_userDefinedRoles.put(filterInfo.userDefinedRoles[i].name, filterInfo.userDefinedRoles[i]);
                }
            }

            if (filterInfo.usersInfo != null) {
                for (int i = 0; i < filterInfo.usersInfo.length; i++) {
                    GenericPrincipal userInfo = filterInfo.usersInfo[i];
                    userInfo.writeEntries = new HashMap<String, List<GenericPrincipal.Attribute>>();
                    userInfo.readEntries = new HashMap<String, List<GenericPrincipal.Attribute>>();
                    userInfo.executeTasks = new HashMap<String, List<GenericPrincipal.Attribute>>();

                    /** Add restricted entries from user-defined role */
                    for (Iterator<String> iter = userInfo.userDefinedRoles.iterator(); iter.hasNext(); ) {
                        String roleName = iter.next();
                        UserDefinedRole userRole = m_userDefinedRoles.get(roleName);
                        if (userRole != null && userRole.restrictedEntries != null) {
                            for (Iterator<String> it = userRole.restrictedEntries.keySet().iterator(); it.hasNext(); ) {
                                String entryType = it.next();
                                List<GenericPrincipal.Attribute> newAttributes = userRole.restrictedEntries.get(entryType);

                                // CHECK WRIRE ROLE
                                if (userRole.inheritRoles.contains(String.valueOf(SecurityContext.PERMISSION_WRITE))) {
                                    List<GenericPrincipal.Attribute> attributes = userInfo.writeEntries.get(entryType);
                                    if (attributes == null) {
                                        attributes = new ArrayList<GenericPrincipal.Attribute>();
                                        userInfo.writeEntries.put(entryType, attributes);
                                    }

                                    for (int j = 0; j < newAttributes.size(); j++)
                                        attributes.add(newAttributes.get(j));
                                }

                                // CHECK READ ROLE
                                if (userRole.inheritRoles.contains(String.valueOf(SecurityContext.PERMISSION_READ))) {
                                    List<GenericPrincipal.Attribute> attributes = userInfo.readEntries.get(entryType);
                                    if (attributes == null) {
                                        attributes = new ArrayList<GenericPrincipal.Attribute>();
                                        userInfo.readEntries.put(entryType, attributes);
                                    }

                                    for (int j = 0; j < newAttributes.size(); j++)
                                        attributes.add(newAttributes.get(j));
                                }

                                // CHECK EXECUTE ROLE
                                if (userRole.inheritRoles.contains(String.valueOf(SecurityContext.PERMISSION_EXECUTE))) {
                                    List<GenericPrincipal.Attribute> attributes = userInfo.executeTasks.get(entryType);
                                    if (attributes == null) {
                                        attributes = new ArrayList<GenericPrincipal.Attribute>();
                                        userInfo.executeTasks.put(entryType, attributes);
                                    }

                                    for (int j = 0; j < newAttributes.size(); j++)
                                        attributes.add(newAttributes.get(j));
                                }
                            }
                            //userDefinedRoles.add(roleName);
                            //rolesList.add(roleName);
                        }
                    }

                    m_userPrincipal.put(userInfo.getUserName(), userInfo);
                }
            }
        } // if (filterInfo != null)

        setSystemUserAccount();
    }

    /**
     * Return the Principal associated with the specified username and credentials, if there is one;
     * otherwise throws SecurityException.
     *
     * @param username Username of the Principal to look up
     * @param password Password or other credentials to use in authenticating this username.
     */
    public GenericPrincipal authenticate(String username, String password) {
        GenericPrincipal principal = m_userPrincipal.get(username);

        if (principal != null) {
            if (!password.equals(principal.getPassword()))
                throw new SpaceSecurityException("Wrong password for user name: " + username);
        } else
            throw new SpaceSecurityException("Unknown user name: " + username);

        return principal;
    }

    public void addUserAccount(GenericPrincipal account) {
        if (m_userPrincipal != null)
            m_userPrincipal.put(account.getUserName(), account);
    }

    public boolean containsUserAccount(String userName) {
        if (m_userPrincipal != null && userName != null)
            return m_userPrincipal.containsKey(userName);
        return false;
    }

    public GenericPrincipal getUserAccount(String userName) {
        return m_userPrincipal.get(userName);
    }

    public void removeUserAccount(String userName) {
        if (m_userPrincipal != null)
            m_userPrincipal.remove(userName);
    }

    public void updateUserAccount(GenericPrincipal account) {
        if (m_userPrincipal != null)
            m_userPrincipal.put(account.getUserName(), account);
    }

    /**
     * Randomly create a system user for internal(server side) purposes only.
     */
    private static final String m_SystemUser = String.valueOf(System.currentTimeMillis());

    /**
     * Set the system user for internal(server side) purposes only.
     */
    private void setSystemUserAccount() {
        GenericPrincipal user = new GenericPrincipal(
                m_SystemUser, m_encrypter.encrypt(m_SystemUser));
        user.setHidden(true);
        addUserAccount(user);
    }

    /**
     * Creates an encrypted data file, that used by <code>com.j_spaces.core.filters.MemoryRealm</code>,
     * it contains space user details and user defined roles.
     *
     * @param newFilterInfo - Security filter data, or <code>null</code> - to create an empty one
     */
    public void createFile(FiltersInfo newFilterInfo)
            throws Exception {
        filterInfo = newFilterInfo;
        flush(newFilterInfo);
    }

    /**
     * Deletes the file if exists on the local file-system.
     *
     * @return <code>true</code> if successfully deleted; <code>false</code> otherwise.
     */
    public boolean deleteFile() {
        if (filterInfo == null)
            return false;

        String fileName = getRootDir() + getUrlPath(filterInfo.paramURL);
        File f = new File(fileName);
        return f.delete();
    }

    /**
     * Creates an encrypted data file, that used by <code>com.j_spaces.core.filters.MemoryRealm</code>,
     * it contains space user details and user defined roles.
     */
    public void flush()
            throws Exception {
        if (filterInfo == null)
            return;

        filterInfo.usersInfo = m_userPrincipal.values().toArray(new GenericPrincipal[m_userPrincipal.size()]);
        filterInfo.userDefinedRoles = m_userDefinedRoles.values().toArray(new UserDefinedRole[m_userDefinedRoles.size()]);

        flush(filterInfo);
    }

    private void flush(FiltersInfo filterInfoToFlush) throws Exception {
        String rootDir = getRootDir();
        String urlPath = getUrlPath(filterInfoToFlush.paramURL);
        File f = new File(rootDir + urlPath);
        if (!f.exists()) {
            //create all directories parent to this file-name
            String dirs = urlPath.substring(0, urlPath.lastIndexOf("/"));
            File d = new File(rootDir + dirs);
            if (!d.mkdirs()) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Failed to create : " + rootDir + urlPath);
                }
            }
        }

        if (m_encrypter == null)
            m_encrypter = DesEncrypter.getInstance();

        FileOutputStream fos = new FileOutputStream(f);
        byte[] byteArray = IOUtils.objectToByteBuffer(filterInfoToFlush);
        byteArray = m_encrypter.encrypt(byteArray);
        fos.write(byteArray);
        fos.flush();
        fos.close();
    }

    /**
     * Returns the root directory, under which /security folder can be found.
     *
     * @return The root directory as defined by "com.gs.security.file.dir" system property; or the
     * home directory.
     */
    public static String getRootDir() {
        String rootDir = System.getProperty("com.gs.security.file.dir", SystemInfo.singleton().getXapHome() + File.separator);
        return rootDir;
    }

    /**
     * Returns the resource url or the default url if non is defined. Note: currently the url is
     * limited to a file-name.
     *
     * @return The resource url as defined by "com.gs.security.file.name" system property; or the
     * default "default-users".
     */
    public static String getResourceUrl(String defaults) {
        String resourceUrl = System.getProperty("com.gs.security.file.name", defaults);
        return resourceUrl;
    }

    /**
     * If the url doesn't denote a directory, use the default "security" directory. Otherwise, it
     * already denotes a directory, so just return it.
     *
     * @return the path to the security file
     */
    public static String getUrlPath(String url) {
        if (!url.startsWith("/")) {
            return "/" + "security" + "/" + url;
        }
        return url;
    }

}/* MemoryRealm class */