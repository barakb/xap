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
 * @(#)JProperties.java	1.0 20/08/2000
 */

package com.j_spaces.kernel.log;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.XmlUtils;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.client.SpaceInitializationException;
import com.j_spaces.core.exception.SpaceConfigurationException;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import static com.j_spaces.core.Constants.SystemTime.SYSTEM_TIME_PROVIDER_PROP;

/**
 * This is a <code>JProperties</code> utility class that can be used by opening an input stream from
 * a XML property file using a URL path that receives as parameter in <code>setURL()</code>
 * function. <code>setURL()</code> function converting XML property file to standard property list.
 * <code>initMessageID()</code> function load standard property file to property list. <p> To use
 * this class, first the user should care to call the following method <code>setURL()</code> with
 * certain URL name, else the user will get <code>NullPointerException</code>. Each key and its
 * corresponding value in the property list is a string.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/
@com.gigaspaces.api.InternalApi
public class JProperties {
    // Container properties map properties, key is container name, 
    //value is Property instance with container properties
    private static Map<String, Properties> _containerPropertiesMap =
            new HashMap<String, Properties>();

    //default space properties - from schema file
    private static final Properties spaceProperties = new Properties();

    //properties passed from the client and which used to overwrite properties or system 
    //properties of the space/container/cluster configuration files
    private static Properties m_customProperties;

    // Spaces properties, key is space name, value is Property instance 
    private static final Map<String, Properties> m_spaceProperties = new Hashtable<String, Properties>();

    // Current URL name
    private static String m_sUri;

    // URLs of all spaces
    private static final Hashtable<String, String> m_UrlTable = new Hashtable<String, String>();

    /**
     * Don't let anyone instantiate this class
     */
    private JProperties() {
    }


    /**
     * Set URL name of property file for opening input stream. param containerName
     *
     * @param uriName URI name where stored property file.
     * @throws SAXException                 This Exception can contain basic error or warning
     *                                      information from either the XML parser or the
     *                                      application
     * @throws ParserConfigurationException Parser Configuration Exception
     * @throws IOException                  If an error occurred during reading from this input
     *                                      stream.
     * @see JProperties#getURL()
     **/
    public static void setURL(String containerName, String uriName)
            throws SAXException, ParserConfigurationException, IOException {
        m_sUri = uriName;

        reload(containerName, null, null);//using uri and not InputStream
    }

    /**
     * Set the InputStream to the config file for resolving the xml file. This method is used when
     * we just call getResourceAsStream() without actually writing it to disk.
     *
     * @param is The InputStream to the prop file where the property file is stored.
     * @throws SAXException                 This Exception can contain basic error or warning
     *                                      information from either the XML parser or the
     *                                      application
     * @throws ParserConfigurationException Parser Configuration Exception
     * @throws IOException                  If an error occurred during reading from this input
     *                                      stream.
     **/
    public static void setInputStream(String containerName,
                                      InputStream is, Properties customProps)
            throws SAXException, ParserConfigurationException, IOException {
        //TODO do we need to put null in the m_sUri in this case ?

        reload(containerName, is, customProps);//using InputStream to reload the xml file
    }


    private static HashMap<String, JSpaceAttributes> dCacheConfigFilesMap = new HashMap<String, JSpaceAttributes>();

    /**
     * Returns the DCache configuration as specified in the file. Changes on the returned Properties
     * object will not affect the original data.
     *
     * @param fileName the name of the configuration file
     */
    public static synchronized JSpaceAttributes loadConfigDCache(String fileName)
            throws SpaceConfigurationException, IOException, SAXException, ParserConfigurationException {
        // check if the configuration is already loaded
        JSpaceAttributes prop = dCacheConfigFilesMap.get(fileName);

        // if prop is null we need to load the configuration
        if (prop == null) {
            // get the InputStream of the file
            InputStream dCacheInputStream = ResourceLoader.findConfigDCache(fileName);
            if (dCacheInputStream != null) {
                // parse the file and convert container custom properties
                prop = (JSpaceAttributes) JProperties.convertXML(dCacheInputStream);
                // save the properties in the map
                dCacheConfigFilesMap.put(fileName, prop);
            }
        }

        // return the configuration
        return (JSpaceAttributes) prop.clone();
    }


    /**
     * Convert XML file of specific space to <code>Properties</code> object and store in public
     * table.
     **/
    public static void setUrlWithSchema(String fullSpaceName, Properties customProperties, InputStream is)
            throws SAXException, ParserConfigurationException, IOException {
        m_customProperties = customProperties;

        DocumentBuilder docBuilder = XmlUtils.getDocumentBuilder();
        Document doc = docBuilder.parse(is);
        Element root = doc.getDocumentElement();

        //id 
        if (spaceProperties.isEmpty())
            convertXMLtoProperty(root, spaceProperties, false);

        Node spaceNameNode = doc.createElement(fullSpaceName);
        spaceNameNode.appendChild(root);

        //JSpaceUtilities.normalize( spaceNameNode );

        // Recursive function that converting XML file to Property list
        JSpaceAttributes sp = new JSpaceAttributes();
        //convert XML file of space to <code>properties</code> object
        convertXMLtoProperty(spaceNameNode, sp, false);
        convertCustomSpacePropsToJProperties(fullSpaceName, customProperties, sp);

        // load the DCache configuration
        String configFileName = sp.getProperty(fullSpaceName + "." + Constants.SPACE_CONFIG_PREFIX + Constants.DCache.CONFIG_NAME_PROP);
        JSpaceAttributes dCacheProperties = loadConfigDCache(configFileName);
        JProperties.convertCustomSpacePropsToJProperties(null, customProperties, dCacheProperties);
        sp.setDCacheProperties(dCacheProperties);
        sp.setDCacheConfigName(configFileName);

        m_spaceProperties.put(fullSpaceName, sp);
    }

    public static void setUrlWithoutSchema(String fullSpaceName, Properties customProperties, String urlName)
            throws SAXException, ParserConfigurationException, IOException {
        m_customProperties = customProperties;

        JSpaceAttributes sp = convertXML(urlName, false);
        convertCustomSpacePropsToJProperties(fullSpaceName, customProperties, sp);
        // DCache - in this case the Dcache configuration is the space configuration
        sp.setDCacheProperties(sp);
        sp.setDCacheConfigName(null);
        m_spaceProperties.put(fullSpaceName, sp);
        m_UrlTable.put(fullSpaceName, urlName);
    }

    /**
     * @return All Spaces xml file configuration Xpath representation and theirs values.
     */
    public static String getAllSpacesPropertiesDump() {
        StringBuilder sb = new StringBuilder();

        if (m_spaceProperties != null) {
            Set<String> spacesSet = m_spaceProperties.keySet();
            sb.append("\n\n==============================================================================================\n");
            sb.append("Spaces configuration elements used by the system\n");
            sb.append("\n---------------------------------------------------------------------------------------- \n\n");
            for (String spaceNameKey : spacesSet) {
                sb.append("\n---------------------------------------------------------------------------------------- \n");
                sb.append("Space configuration elements for space < ");
                sb.append(spaceNameKey);
                sb.append(" >");
                sb.append("\n---------------------------------------------------------------------------------------- \n\n");
                StringUtils.appendProperties(sb, m_spaceProperties.get(spaceNameKey));
            }
        }

        return sb.toString();
    }

    /**
     * @return Container xml file configuration Xpath representation and theirs values. if the value
     * is a system prop var e.g. ${xxx} and it is NOT been set we show the default value behind that
     * property key as defined in the system constants.
     */
    public static String getContainerPropertiesDump(String containerName) {
        StringBuilder dumpInfo = new StringBuilder();
        Properties m_properties = _containerPropertiesMap.get(containerName);
        if (m_properties != null) {
            dumpInfo.append("\n\n==============================================================================================\n");
            dumpInfo.append("Container configuration elements for container < " +
                    m_properties.getProperty(Constants.Container.CONTAINER_NAME_PROP) + " >\n");
            dumpInfo.append("\n---------------------------------------------------------------------------------------- \n\n");

            for (Enumeration e = m_properties.propertyNames(); e.hasMoreElements(); ) {
                String contPropKey = (String) e.nextElement();
                String propValue = m_properties.getProperty(contPropKey);
                //if the value is a system prop var e.g. ${xxx} and it is NOT been set
                //we show the default value behind that key as defined in the system.
                //TODO this needs to be improved to have a nice mapping of key and its default value in the system.
                String resolvedPropertyValue = propValue;
                if (propValue.equalsIgnoreCase("${" + SystemProperties.START_EMBEDDED_LOOKUP + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, SystemProperties.START_EMBEDDED_LOOKUP_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.START_EMBEDDED_MAHALO + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, SystemProperties.START_EMBEDDED_MAHALO_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.JINI_LUS_GROUPS + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, SystemInfo.singleton().lookup().defaultGroups());
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.CONTAINER_SHUTDOWN_HOOK + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, Constants.Container.CONTAINER_SHUTDOWN_HOOK_PROP_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.LOOKUP_JNDI_URL + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, Constants.LookupManager.LOOKUP_JNDI_URL_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.LOOKUP_UNICAST_ENABLED + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, Constants.LookupManager.LOOKUP_UNICAST_ENABLED_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.JINI_LUS_LOCATORS + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, SystemProperties.JINI_LUS_LOCATORS_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.SYSTEM_TIME_PROVIDER + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.JMS_LOOKUP_ENABLED + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, Constants.LookupManager.LOOKUP_JMS_ENABLED_DEFAULT);
                } else if (propValue.equalsIgnoreCase("${" + SystemProperties.GS_PROTOCOL + "}")) {
                    resolvedPropertyValue = getPropertyFromSystem(propValue, SystemProperties.GS_PROTOCOL_DEFAULT);
                }

//          dumpInfo.append("Container config element ");
                dumpInfo.append("\n\t XPath element key: " + contPropKey);
                dumpInfo.append("\n\t Value: " + propValue);
                if (!resolvedPropertyValue.equalsIgnoreCase(propValue))//if the value was resolved from system prop we show its default
                    dumpInfo.append("\n\t Default value is set to: " + resolvedPropertyValue);

                dumpInfo.append("\n");
            }
        }
        return dumpInfo.toString();
    }

    /**
     * Convert space attributes according to custom properties
     *
     * @param spaceName   if space name is null then use XPath as key (i.e in the case of DCache
     *                    config)
     * @param customProps custom properties
     * @param spaceAttrib space attributes
     */
    public static void convertCustomSpacePropsToJProperties(String spaceName,
                                                            Properties customProps, JSpaceAttributes spaceAttrib) {
        if (customProps != null) {
            for (Enumeration e = customProps.propertyNames(); e.hasMoreElements(); ) {
                String propName = (String) e.nextElement();
                String propValue = customProps.getProperty(propName);
                String spacePropKey = spaceName == null ? propName : spaceName + "." + propName;

				/*
                 * When the key to be overwritten is a space config key, i.e. starts
				 * with space-config, we set its new value. 
				 * 
				 * Note: previously, we only set the value if the spaceAttrib contained the key
				 * to be overwritten. Now, we set it regardless. see APP-50
				 */
                if (propName.startsWith(Constants.SPACE_CONFIG_PREFIX))
                    spaceAttrib.setProperty(spacePropKey, propValue);
            }
        }
    }

    private static void convertCustomContainerPropsToJProperties(Properties customProps, JSpaceAttributes spaceAttrib) {
        if (customProps != null) {
            for (Enumeration e = customProps.propertyNames(); e.hasMoreElements(); ) {
                String contPropKey = (String) e.nextElement();
                String propValue = customProps.getProperty(contPropKey);

            /*
                 * When the key to be overwritten is NOT a space config key (but a
				 * container config key), i.e. does NOT start with space-config, we
				 * set its new value. 
				 * 
				 * Note: previously, we only set the value if the container spaceAttrib
				 * contained the key to be overwritten. Now, we set it regardless. see APP-50
				 */
                if (!contPropKey.startsWith(Constants.SPACE_CONFIG_PREFIX)) {
                    spaceAttrib.setProperty(contPropKey, propValue);
                }
            }
        }
    }


    /**
     * Convert XML file of specific space to <code>Properties</code> object and store in public table.
     *
     * @param spaceName The space name
     * @param urlName URL name where stored property file. If the input stream != null, we ignore the url.
     * @param is if != null we use this while parsing xml, otherwise we use the URL.
     * @param useSchema if true then meaning the urlName is a path to a space schema file
     * 		, in that case we use this xml file as the template schema of the specific space name.
     * 		If false, we do not look for schema and use the provided specific space xml file path.
     * @exception SAXException   This Exception can contain basic error or
     *  warning information from either the XML parser or the application
     * @exception ParserConfigurationException   Parser Configuration Exception
     * @exception IOException   If an error occurred during reading from this input stream.
     *
     * @see com.j_spaces.kernel.JProperties#getURL(String spaceName)
     **/
   /*
    public static void setDefaultSpaceProperties ( InputStream is )
           throws SAXException, ParserConfigurationException, IOException {
    	
       Properties sp = null;
	
       //replace the <space-name> tag with the actual space name
       //Obtaining a org.w3c.dom.Document from XML
       DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
       DocumentBuilder builder = factory.newDocumentBuilder();


        Document doc = builder.parse( is );

        Element root = doc.getDocumentElement();
          
        // Recursive function that converting XML file to Property list
    	//sp = new JSpaceAttributes();
        //convert XML file of space to <code>properties</code> object
    	convertXMLtoProperty( root, space_Properties, false );
   }   
    */


    /**
     * Returns current URL name of container XML file.
     *
     * @return Current URL name.
     * @see com.j_spaces.kernel.JProperties#setURL(java.lang.String)
     **/
    public static String getURL() {
        return m_sUri;
    }

    /**
     * Returns current URL name of space XML file.
     *
     * @param fullSpaceName Full space name: <ContainerName>:<SpaceName>
     * @return Current URL name.
     * @see com.j_spaces.kernel.JProperties#setURL(String spaceName, String urlName)
     **/
    public static String getURL(String fullSpaceName) {
        return m_UrlTable.get(fullSpaceName);
    }


    /**
     * Remove <code>Properties</code> object of space from JProperties.
     *
     * @param fullSpaceName Full space name: <ContainerName>:<SpaceName>
     **/
    public static void removeSpaceProperties(String fullSpaceName) {
        // remove from URL table
        m_UrlTable.remove(fullSpaceName);

        // remove from properties table
        m_spaceProperties.remove(fullSpaceName);
    }


    /**
     * Reopening the URL connection from property file.
     *
     * @param is if null we use the uri to parse the xml file.
     * @throws SAXException                 This Exception can contain basic error or warning
     *                                      information from either the XML parser or the
     *                                      application
     * @throws ParserConfigurationException Parser Configuration Exception
     * @throws IOException                  If an error occurred during reading from this input
     *                                      stream.
     * @see com.j_spaces.kernel.JProperties#setURL(java.lang.String)
     **/
    private static void reload(String containerName, InputStream is, Properties customProps)
            throws SAXException, ParserConfigurationException, IOException {
        m_customProperties = customProps;
        Properties m_properties;
        if (is != null) {
            m_properties = convertXML(is, false, customProps);
        } else {
            m_properties = convertXML(m_sUri, false);
        }

        _containerPropertiesMap.put(containerName, m_properties);
    }


    /**
     * Returns current <code>Properties</code> object.
     *
     * @return This <code>Properties</code> object.
     **/
    public static Properties getContainerProperties(String containerName) {
        return _containerPropertiesMap.get(containerName);
    }

    /**
     * Searches for the property with the specified key in this property list of container. If the
     * key is not found in this property list the method returns the default value argument.
     *
     * @param key          Certain key.
     * @param defaultValue A default value.
     * @return the value from the property list with the specified key value.
     **/
    public static String getContainerProperty(String containerName,
                                              String key, String defaultValue) {
        return getContainerProperty(containerName, key, defaultValue, true);
    }

    /**
     * Retrieve container properties according to its name.
     *
     * @param containerName if name is null any container will be taken. That is done only for
     *                      system time property.
     * @param key           container property name
     * @param convert       indicates if convert with system property
     * @return container property value
     */
    public static String getContainerProperty(String containerName,
                                              String key,
                                              String defaultValue,
                                              boolean convert) {
        Properties m_properties = null;
        if (containerName != null) {
            m_properties = _containerPropertiesMap.get(containerName);
        }
        //Container name can be null only for cross container property as "system time"
        //in such case first found container Properties object will be taken
        else if (key.equals(SYSTEM_TIME_PROVIDER_PROP)) {
            //return default value if map is empty
            if (_containerPropertiesMap.size() == 0) {
                return defaultValue;
            }
            Collection<Properties> values = _containerPropertiesMap.values();
            m_properties = values.iterator().next();
        }

        if (m_properties == null) {
            return null;
        }

        String result = m_properties.getProperty(key, defaultValue);
        return ((convert) ? getPropertyFromSystem(result, defaultValue) : result);
    }

    public static boolean containsSpaceProperty(String fullSpaceName, String key) {
        Properties sp = m_spaceProperties.get(fullSpaceName);
        if (sp != null) {
            if (sp.containsKey(fullSpaceName + "." + Constants.SPACE_CONFIG_PREFIX + key))
                return true;
            if (sp.containsKey(Constants.SPACE_CONFIG_PREFIX + key))
                return true;
        }

        return false;
    }

    /**
     * Returns Properties of certain space.
     *
     * @param fullSpaceName Full space name: <ContainerName>:<SpaceName>
     **/
    public static Properties getSpaceProperties(String fullSpaceName) {
        return m_spaceProperties.get(fullSpaceName);
    }

    public static void setSpaceProperties(String fullSpaceName, Properties properties) {
        m_spaceProperties.put(fullSpaceName, properties);
    }

    public static SpaceConfigReader loadDCacheConfig(ISpaceProxy remoteSpace, Properties customProperties, String dcacheSuffix)
            throws SpaceInitializationException {
        try {
            final String fullDCacheSpaceName = JSpaceUtilities.createFullSpaceName(remoteSpace.getContainerName(), remoteSpace.getName() + dcacheSuffix);

            // get configuration of master space
            final SpaceConfig remoteSpaceProperties = ((IRemoteJSpaceAdmin) remoteSpace.getPrivilegedAdmin()).getConfig();
            JSpaceAttributes dcacheProperties = remoteSpaceProperties.getDCacheProperties();

            final String dCacheConfigName = remoteSpaceProperties.getDCacheConfigName();
            final InputStream dCacheInputStream = ResourceLoader.findConfigDCache(dCacheConfigName);
            if (dCacheInputStream != null) {
                m_customProperties = customProperties;

                dcacheProperties = convertXML(dCacheInputStream, false, customProperties);
                convertCustomSpacePropsToJProperties(fullDCacheSpaceName, customProperties, dcacheProperties);
                // DCache - in this case the Dcache configuration is the space configuration
                dcacheProperties.setDCacheProperties(dcacheProperties);
                dcacheProperties.setDCacheConfigName(null);
            }

            // set local-cache-mode=true for DCache Embedded Space
            dcacheProperties.setProperty(Constants.Engine.ENGINE_LOCAL_CACHE_MODE_PROP, Boolean.TRUE.toString());
            m_spaceProperties.put(fullDCacheSpaceName, dcacheProperties);
            return new SpaceConfigReader(fullDCacheSpaceName);
        } catch (SAXException e) {
            throw new SpaceInitializationException("Failed to load space cache configuration", e);
        } catch (ParserConfigurationException e) {
            throw new SpaceInitializationException("Failed to load space cache configuration", e);
        } catch (IOException e) {
            throw new SpaceInitializationException("Failed to load space cache configuration", e);
        }
    }

    public static String getSpaceProperty(String fullSpaceName, String key, String defaultValue) {
        return getSpaceProperty(fullSpaceName, key, defaultValue, true);
    }

    public static String getSpaceProperty(String fullSpaceName, String key, String defaultValue, boolean convert) {
        Properties sp = m_spaceProperties.get(fullSpaceName);

        String result = null;
        if (sp != null) {
            result = sp.getProperty(fullSpaceName + "." + Constants.SPACE_CONFIG_PREFIX + key, null);
            if (result == null)
                result = sp.getProperty(Constants.SPACE_CONFIG_PREFIX + key, null);
        }

        if (result == null)
            result = defaultValue;

        if (convert)
            result = getPropertyFromSystem(result, defaultValue);

        return result;
    }

    public static <T> T getObjectSpaceProperty(String fullSpaceName, String key) {
        Properties sp = m_spaceProperties.get(fullSpaceName);

        T result = null;
        if (sp != null) {
            result = (T) sp.get(fullSpaceName + "." + Constants.SPACE_CONFIG_PREFIX + key);
            if (result == null)
                result = (T) sp.getProperty(Constants.SPACE_CONFIG_PREFIX + key, null);
        }

        return result;
    }

    public static void setSpaceProperty(String fullSpaceName, String key, String value) {
        Properties sp = m_spaceProperties.get(fullSpaceName);
        if (sp != null)
            sp.setProperty(fullSpaceName + "." + Constants.SPACE_CONFIG_PREFIX + key, value);
    }

    /**
     * Convert XML file to <code>Properties</code> object.
     *
     * @param uri                       <code>URI</code> of XML file for conversion.
     * @param isConvertToSystemProperty Is convert xml properties from System properties.
     * @return Property  <code>Properties</code> object that converted from XML file.
     **/
    public static JSpaceAttributes convertXML(String uri, boolean isConvertToSystemProperty)
            throws SAXException, ParserConfigurationException, IOException {
        /**
         * Parse the content of the given InputStream as an XML document and
         * return a new DOM Document object.
         **/
        Document doc = XmlUtils.getDocumentBuilder().parse(uri);
        Element root = doc.getDocumentElement();

        // Recursive function that converting XML file to Property list
        JSpaceAttributes prop = new JSpaceAttributes();
        convertXMLtoProperty(root, prop, isConvertToSystemProperty);
        return prop;
    }

    /**
     * Convert XML file to <code>Properties</code> object.
     *
     * @param is <code>InputStream</code> of XML file for conversion. By default converts xml
     *           properties from System properties.
     * @return Property  <code>Properties</code> object that was converted from XML file.
     */
    public static Properties convertXML(InputStream is)
            throws SAXException, ParserConfigurationException, IOException {
        return convertXML(is, true, null);
    }

    /**
     * Convert XML file to <code>Properties</code> object.
     *
     * @param is                        <code>InputStream</code> of XML file for conversion.
     * @param isConvertToSystemProperty Is convert xml properties from System properties.
     * @return Property  <code>Properties</code> object that was converted from XML file.
     **/
    public synchronized static JSpaceAttributes convertXML(InputStream is, boolean isConvertToSystemProperty, Properties customProps)
            throws SAXException, ParserConfigurationException, IOException {
        // Obtaining a org.w3c.dom.Document from XML
       /*DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
       DocumentBuilder builder = factory.newDocumentBuilder();
*/
        /**
         * Parse the content of the given InputStream as an XML document and
         * return a new DOM Document object.
         **/
        Document doc = XmlUtils.getDocumentBuilder().parse(is);
        Element root = doc.getDocumentElement();

        // Recursive function that converting XML file to Property list
        JSpaceAttributes prop = new JSpaceAttributes();
        convertXMLtoProperty(root, prop, isConvertToSystemProperty);
        convertCustomContainerPropsToJProperties(customProps, prop);
        return prop;
    }

    /**
     * Recursive function thats converting XML file to Property list.
     *
     * @param n The Node interface is the primary datatype for the entire Document Object Model.
     **/
    private static void convertXMLtoProperty(Node n, Map prop, boolean isConvertToSystemProperty) {
        NodeList nl = n.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            Node node = nl.item(i);

            // Check whether this Node type is TEXT.
            if (node.getNodeType() == Node.TEXT_NODE && node.getNodeValue().trim().length() > 0)
                buildProperty(node, prop, isConvertToSystemProperty); // Building property value and key string.
            else
                convertXMLtoProperty(node, prop, isConvertToSystemProperty);
        } /* for */
    } /* convertXMLtoProperty */

    /**
     * This method build fill <code>Properties</code> from XML structure. For example you want to
     * convert the following XML form to property form: <a> <b> <c>Test<c> <b> <a> After converting
     * you got the following property: a.b.c=Test;
     *
     * @param node Current Node.
     * @param prop The property object to fill.
     */
    private static void buildProperty(Node node, Map<String, String> prop, boolean isConvertFromSystemProperty) {
        // This Stack contains all names of Parent-Nodes
        Stack<String> inverseKey = new Stack<String>();
        StringBuilder key = new StringBuilder();
        String propertyValue = node.getNodeValue();

        // Push to stack all Parents Nodes
        while (node.getParentNode() != null && node.getParentNode().getNodeType() != Node.DOCUMENT_NODE) {
            inverseKey.push(node.getParentNode().getNodeName());
            node = node.getParentNode();
        }

        // Complete "." for every Parent Node for building key property
        while (!inverseKey.isEmpty()) {
            key.append(inverseKey.pop()).append('.');
        }

        // Delete last "." symbol
        if (key.charAt(key.length() - 1) == '.') {
            key = key.deleteCharAt(key.length() - 1);
        }

        // trim the string
        propertyValue = propertyValue.trim();

        // convert from system property
        if (isConvertFromSystemProperty)
            propertyValue = getPropertyFromSystem(propertyValue);

        // Puts the key and value to HashTable
        String propKey = key.toString();
        ///test
//      if( propKey.indexOf( Constants.Schemas.SCHEMA_ELEMENT ) != -1 ){
//    	  System.out.println( "!!!!!!" );
//      }


        prop.put(propKey, propertyValue);
    }

    /**
     * Replace the XML propertyValue with System property value if contains. For example the value
     * is: ${com.gs.home}/logs/services/pc-igor-rmid.log This method will check if the following
     * System property defined in JVM -Dcom.gs.home if yes, the system property will be replaced in
     * propertyValue: c:/gigaspaces2.5/logs/services/pc-igor-rmid.log, otherwise will return the
     * propertyValue without changes.
     *
     * If the property was not found as system property we try to get it from the custom properties
     * object passed by the client. If still none, the XML propertyValue is returned.
     *
     * @param propertyValue The XML property value.
     * @return Converted propertyValue with System property.
     * @see #getPropertyFromSystem(String, String)
     * @since version 5.0
     **/
    static public String getPropertyFromSystem(String propertyValue) {
        return getPropertyFromSystem(propertyValue, null /*don't use default value*/);
    }

    /**
     * Replace the XML propertyValue with System property value if contains. For example the value
     * is: ${com.gs.home}/logs/services/pc-igor-rmid.log This method will check if the following
     * System property defined in JVM -Dcom.gs.home if yes, the system property will be replaced in
     * propertyValue: c:/gigaspaces2.5/logs/services/pc-igor-rmid.log, otherwise will return the
     * propertyValue without changes.
     *
     * If the property was not found as system property we try to get it from the custom properties
     * object passed by the client. If still none, and no default value, the XML propertyValue is
     * returned.
     *
     * @param propertyValue The XML property value.
     * @param defValue      Default value, When <tt>null</tt> default value is ignored.
     * @return Converted propertyValue with System property.
     * @since version 2.5
     **/
    static public String getPropertyFromSystem(String propertyValue, String defValue) {
        if (propertyValue == null)
            return null;

        // search System property definition
        int fIndex = propertyValue.indexOf("${");
        int lIndex = propertyValue.lastIndexOf("}");

        if (fIndex != -1 && lIndex != -1) {
            String cuttedkey = propertyValue.substring(fIndex + 2, lIndex);
            boolean useDefValue = (defValue != null);
            StringBuilder sb = new StringBuilder(propertyValue);
            // get system property if exist
            String sysProper;
            if (useDefValue) {
                sysProper = System.getProperty(cuttedkey, defValue);
            } else {
                sysProper = System.getProperty(cuttedkey);
            }

            //21.11.05 - Gershon:
            //as last chance, only if sys prop was NOT set.
            //Lets try and find this property in the custom properties which passed by the client
            //and suppose to overwrite common configurations.
            if (sysProper == null && m_customProperties != null) {
                //check if we have SOME_KEY key in the properties object
                if (m_customProperties.containsKey(cuttedkey)) {
                    if (useDefValue) {
                        sysProper = m_customProperties.getProperty(cuttedkey, defValue);
                    } else {
                        sysProper = m_customProperties.getProperty(cuttedkey);
                    }
                }
                //check if we have ${SOME_KEY} key in the properties object
                else if (m_customProperties.containsKey(propertyValue)) {
                    if (useDefValue) {
                        sysProper = m_customProperties.getProperty(propertyValue, defValue);
                    } else {
                        sysProper = m_customProperties.getProperty(propertyValue);
                    }
                }
            }

            if (sysProper != null)
                propertyValue = sb.replace(fIndex, lIndex + 1, sysProper).toString().replace('\\', '/');
        }

        return propertyValue;
    }

    static public String resolveSystemPropertyName(String propertyValue) {
        String systemPropertyName = "";

        if (propertyValue == null)
            return systemPropertyName;

        // search System property definition
        int fIndex = propertyValue.indexOf("${");
        int lIndex = propertyValue.lastIndexOf("}");

        if (fIndex != -1 && lIndex != -1) {
            // get system property if exist
            systemPropertyName = propertyValue.substring(fIndex + 2, lIndex);
        }

        return systemPropertyName;
    }

    public static void main(String[] args) {
        //String str = "${com.gs.home}/GenericPersistProperties/${com.gs.container.name}_${com.gs.space.name}" +
        //		 "/${com.gs.container.name}_${com.gs.space.name}DB.dbs";

        String str = "../GenericPersistProperties/${com.gs.container.name}_${com.gs.space.name}/${com.gs.container.name}_${com.gs.space.name}DB.dbs";


        System.setProperty("com.gs.container.name", "CONTAINERTEST");
        System.setProperty("com.gs.space.name", "SPACETEST");

        String result = getPropertyFromSystemForComplicatedValue(str, "");
        System.out.println("RESULT" + result);
    }

    static public String getPropertyFromSystemForComplicatedValue(String propertyValue, String defValue) {
        if (propertyValue == null)
            return null;

        int firstOpenIndex = propertyValue.indexOf("${");
        int lastOpenIndex = propertyValue.lastIndexOf("${");

        //check if we have only one system property here
        if (firstOpenIndex == lastOpenIndex)
            return getPropertyFromSystem(propertyValue, defValue);

        StringBuffer resultBuffer = new StringBuffer();
        int beginIndex = 0;

        // if property value is not starting from "${"
        if (firstOpenIndex != 0) {
            String subStr = propertyValue.substring(0, firstOpenIndex);
            resultBuffer.append(subStr);
            beginIndex = firstOpenIndex;
        }

        int nextOpenBracketIndex = firstOpenIndex;

        boolean isStop = false;
        //while stop is false stay in loop
        while (!isStop) {
            ////find open bracket
            nextOpenBracketIndex = propertyValue.indexOf("${", nextOpenBracketIndex + 1);
            if (nextOpenBracketIndex == -1) {
                nextOpenBracketIndex = propertyValue.length();
                isStop = true;
            }

            //take string until next open bracket
            String subString = propertyValue.substring(beginIndex, nextOpenBracketIndex);

            beginIndex = nextOpenBracketIndex;

            String val = getPropertyFromSystem(subString, "");

            resultBuffer.append(val);
        }

        String result = resultBuffer.toString();

        if (result.length() == 0)
            result = defValue;

        return result;
    }

    /**
     * This method checks if value has format of system property
     *
     * @return true if this is system variable i.e. of type ${somekey}
     */
    public static boolean isSystemProp(String propertyValue) {
        int fIndex = propertyValue.indexOf("${");
        int lIndex = propertyValue.lastIndexOf("}");
        return (fIndex != -1 && lIndex != -1);
    }


}