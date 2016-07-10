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

package org.openspaces.persistency.support;

import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Niv Ingberg
 * @since 9.7
 */
public class TypeDescriptorUtils {

    public static List<SpaceTypeDescriptor> sort(Collection<SpaceTypeDescriptor> typeDescriptors) {
        TypeHierarchySorter sorter = new TypeHierarchySorter();
        Map<String, SpaceTypeDescriptor> typeDescriptorsMap = new HashMap<String, SpaceTypeDescriptor>();

        for (SpaceTypeDescriptor typeDescriptor : typeDescriptors) {
            if (null != typeDescriptor) {
                typeDescriptorsMap.put(typeDescriptor.getTypeName(), typeDescriptor);
                sorter.addTypeName(typeDescriptor.getTypeName(), typeDescriptor.getSuperTypeName());
            }
        }

        Map<String, TypeNameNode> allTypeNameNodes = sorter.nodes;
        TypeNameNode root = sorter.fixAndGetRoot();

        List<SpaceTypeDescriptor> result = new LinkedList<SpaceTypeDescriptor>();

        // root is java.lang.Object so we skip him
        for (String typeName : root.children)
            addSelfThenChildren(typeName, typeDescriptorsMap, allTypeNameNodes, result);

        return result;

    }

    /**
     * @param typeDescriptorContainers A map from type name to its matching {@link
     *                                 SpaceTypeDescriptorContainer}
     * @return A list of {@link SpaceTypeDescriptor} instances sorted in such way that super types
     * will precede their sub types.
     */
    public static List<SpaceTypeDescriptor> sort(Map<String, SpaceTypeDescriptorContainer> typeDescriptorContainers) {
        Collection<SpaceTypeDescriptor> typeDescriptors = new HashSet<SpaceTypeDescriptor>();
        for (SpaceTypeDescriptorContainer container : typeDescriptorContainers.values()) {
            typeDescriptors.add(container.getTypeDescriptor());
        }
        return sort(typeDescriptors);
    }

    private static void addSelfThenChildren(String typeName,
                                            Map<String, SpaceTypeDescriptor> typeDescriptors,
                                            Map<String, TypeNameNode> nodes,
                                            List<SpaceTypeDescriptor> result) {
        SpaceTypeDescriptor typeDescriptor = typeDescriptors.get(typeName);
        result.add(typeDescriptor);
        TypeNameNode typeNameNode = nodes.get(typeName);
        for (String childTypeName : typeNameNode.children)
            addSelfThenChildren(childTypeName, typeDescriptors, nodes, result);
    }

    private static class TypeNameNode {
        private final String typeName;
        private String superTypeName;
        private final Set<String> children = new HashSet<String>();

        private TypeNameNode(String typeName, String superTypeName) {
            this.typeName = typeName;
            this.superTypeName = superTypeName;
        }
    }

    private static class TypeHierarchySorter {

        private final TypeNameNode root = new TypeNameNode(Object.class.getName(), null);
        private final Map<String, TypeNameNode> nodes = new HashMap<String, TypeNameNode>();

        private TypeHierarchySorter() {
            nodes.put(root.typeName, root);
        }

        private void addTypeName(String typeName, String superTypeName) {
            TypeNameNode typeNameNode = nodes.get(typeName);
            TypeNameNode superTypeNameNode = nodes.get(superTypeName);

            if (typeNameNode == null) {
                typeNameNode = new TypeNameNode(typeName, superTypeName);
                nodes.put(typeName, typeNameNode);
            } else {
                typeNameNode.superTypeName = superTypeName;
            }

            if (superTypeNameNode == null) {
                superTypeNameNode = new TypeNameNode(superTypeName, Object.class.getName());
            }

            superTypeNameNode.children.add(typeName);
            nodes.put(superTypeName, superTypeNameNode);
        }

        private TypeNameNode fixAndGetRoot() {
            for (TypeNameNode typeNameNode : nodes.values()) {
                if (typeNameNode != root && typeNameNode.superTypeName.equals(root.typeName)) {
                    root.children.add(typeNameNode.typeName);
                }
            }
            return root;
        }
    }
}
