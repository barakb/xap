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


package org.openspaces.core.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A Spring bean post processor allowing to use {@link GigaSpaceContext} in order to inject {@link
 * GigaSpaceContext} instances using annotations.
 *
 * @author kimchy
 * @see org.openspaces.core.context.GigaSpaceContext
 * @see org.openspaces.core.GigaSpace
 */
public class GigaSpaceContextBeanPostProcessor extends InstantiationAwareBeanPostProcessorAdapter implements ApplicationContextAware {

    protected final Log logger = LogFactory.getLog(getClass());

    private ApplicationContext applicationContext;

    private Map<Class<?>, List<AnnotatedMember>> classMetadata = new HashMap<Class<?>, List<AnnotatedMember>>();

    private Map<String, GigaSpace> gsByName;

    private GigaSpace uniqueGs;

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * Lazily initialize gs map.
     */
    private synchronized void initMapsIfNecessary() {
        if (this.gsByName == null) {
            this.gsByName = new HashMap<String, GigaSpace>();
            // Look for named GigaSpaces

            for (String gsName : BeanFactoryUtils.beanNamesForTypeIncludingAncestors(this.applicationContext, GigaSpace.class)) {

                GigaSpace gs = (GigaSpace) this.applicationContext.getBean(gsName);
                gsByName.put(gsName, gs);
            }

            if (this.gsByName.isEmpty()) {
                // Try to find a unique GigaSpaces.
                String[] gsNames = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(this.applicationContext, GigaSpace.class);
                if (gsNames.length == 1) {
                    this.uniqueGs = (GigaSpace) this.applicationContext.getBean(gsNames[0]);
                }
            } else if (this.gsByName.size() == 1) {
                this.uniqueGs = this.gsByName.values().iterator().next();
            }

            if (this.gsByName.isEmpty() && this.uniqueGs == null) {
                logger.warn("No named gs instances defined and not exactly one anonymous one: cannot inject");
            }
        }
    }

    protected GigaSpace findGigaSpaceByName(String gsName) throws NoSuchBeanDefinitionException {

        initMapsIfNecessary();
        if (gsName == null || "".equals(gsName)) {
            if (this.uniqueGs != null) {
                return this.uniqueGs;
            } else {
                throw new NoSuchBeanDefinitionException("No GigaSpaces name given and factory contains several");
            }
        }
        GigaSpace namedGs = this.gsByName.get(gsName);
        if (namedGs == null) {
            throw new NoSuchBeanDefinitionException("No GigaSpaces found for name [" + gsName + "]");
        }
        return namedGs;
    }

    @Override
    public boolean postProcessAfterInstantiation(Object bean, String beanName) throws BeansException {
        if (bean == null) {
            return true;
        }
        List<AnnotatedMember> metadata = findClassMetadata(bean.getClass());
        for (AnnotatedMember member : metadata) {
            member.inject(bean);
        }
        return true;
    }

    private synchronized List<AnnotatedMember> findClassMetadata(Class<?> clazz) {
        List<AnnotatedMember> metadata = this.classMetadata.get(clazz);
        if (metadata == null) {
            final List<AnnotatedMember> newMetadata = new LinkedList<AnnotatedMember>();

            ReflectionUtils.doWithFields(clazz, new ReflectionUtils.FieldCallback() {
                public void doWith(Field f) {
                    addIfPresent(newMetadata, f);
                }
            });

            ReflectionUtils.doWithMethods(clazz, new ReflectionUtils.MethodCallback() {
                public void doWith(Method m) {
                    addIfPresent(newMetadata, m);
                }
            });

            metadata = newMetadata;
            this.classMetadata.put(clazz, metadata);
        }
        return metadata;
    }

    private void addIfPresent(List<AnnotatedMember> metadata, AccessibleObject ao) {
        GigaSpaceContext gsContext = ao.getAnnotation(GigaSpaceContext.class);
        if (gsContext != null) {
            metadata.add(new AnnotatedMember(gsContext.name(), ao));
        }
    }

    /**
     * Class representing injection information about an annotated field or setter method.
     */
    private class AnnotatedMember {

        private final String name;

        private final AccessibleObject member;

        public AnnotatedMember(String name, AccessibleObject member) {
            this.name = name;
            this.member = member;

            // Validate member type
            Class<?> memberType = getMemberType();
            if (!GigaSpace.class.isAssignableFrom(memberType)) {
                throw new IllegalArgumentException("Cannot inject [" + member + "], not a supported GigaSpaces type");
            }
        }

        public void inject(Object instance) {
            Object value = resolve();
            try {
                if (!this.member.isAccessible()) {
                    this.member.setAccessible(true);
                }
                if (this.member instanceof Field) {
                    ((Field) this.member).set(instance, value);
                } else if (this.member instanceof Method) {
                    ((Method) this.member).invoke(instance, value);
                } else {
                    throw new IllegalArgumentException("Cannot inject unknown AccessibleObject type " + this.member);
                }
            } catch (IllegalAccessException ex) {
                throw new IllegalArgumentException("Cannot inject member " + this.member, ex);
            } catch (InvocationTargetException ex) {
                // Method threw an exception
                throw new IllegalArgumentException("Attempt to inject setter method " + this.member
                        + " resulted in an exception", ex);
            }
        }

        /**
         * Return the type of the member, whether it's a field or a method.
         */
        public Class<?> getMemberType() {
            if (member instanceof Field) {
                return ((Field) member).getType();
            } else if (member instanceof Method) {
                Method setter = (Method) member;
                if (setter.getParameterTypes().length != 1) {
                    throw new IllegalArgumentException("Supposed setter " + this.member + " must have 1 argument, not "
                            + setter.getParameterTypes().length);
                }
                return setter.getParameterTypes()[0];
            } else {
                throw new IllegalArgumentException("Unknown AccessibleObject type " + this.member.getClass()
                        + "; Can only inject setter methods or fields");
            }
        }

        /**
         * Resolve the object against the application context.
         */
        protected Object resolve() {
            // Resolves to GigaSpaces
            GigaSpace gs = findGigaSpaceByName(this.name);
            if (GigaSpace.class.isAssignableFrom(getMemberType())) {
                if (!getMemberType().isInstance(gs)) {
                    throw new IllegalArgumentException("Cannot inject " + this.member + " with GigaSpaces ["
                            + this.name + "]: type mismatch");
                }
                return gs;
            } else {
                throw new IllegalArgumentException("Failure to inject");
            }
        }
    }
}
