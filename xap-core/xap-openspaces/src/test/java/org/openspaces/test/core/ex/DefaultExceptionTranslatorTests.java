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


package org.openspaces.test.core.ex;

import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.j_spaces.core.MemoryShortageException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.OperationTimeoutException;

import junit.framework.TestCase;

import net.jini.core.transaction.TransactionException;

import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.EntryNotInSpaceException;
import org.openspaces.core.EntrySerializationException;
import org.openspaces.core.InactiveTransactionException;
import org.openspaces.core.InvalidFifoClassException;
import org.openspaces.core.InvalidFifoTemplateException;
import org.openspaces.core.InvalidTransactionUsageException;
import org.openspaces.core.ObjectConversionException;
import org.openspaces.core.SpaceMemoryShortageException;
import org.openspaces.core.SpaceOptimisticLockingFailureException;
import org.openspaces.core.TransactionDataAccessException;
import org.openspaces.core.UncategorizedSpaceException;
import org.openspaces.core.UnusableEntryException;
import org.openspaces.core.UpdateOperationTimeoutException;
import org.openspaces.core.exception.DefaultExceptionTranslator;
import org.openspaces.core.exception.ExceptionTranslator;
import org.springframework.dao.DataAccessException;

/**
 * @author kimchy
 */
public class DefaultExceptionTranslatorTests extends TestCase {

    private ExceptionTranslator exTranslator;

    protected void setUp() throws Exception {
        exTranslator = new DefaultExceptionTranslator();
    }

    public void testGeneralException() {
        Exception e = new Exception("test");
        DataAccessException dae = exTranslator.translate(e);
        assertEquals(UncategorizedSpaceException.class, dae.getClass());
        assertSame(e, dae.getCause());
    }

    public void testUnusableEntryException() {
        Exception cause = new Exception("cause");
        net.jini.core.entry.UnusableEntryException uee = new net.jini.core.entry.UnusableEntryException(cause);
        DataAccessException dae = exTranslator.translate(uee);
        assertEquals(UnusableEntryException.class, dae.getClass());
        assertSame(uee, dae.getCause());
    }

    public void testEntryAlreadyInSpaceException() {
        DataAccessException dae = exTranslator.translate(new com.j_spaces.core.client.EntryAlreadyInSpaceException("UID", "CLASSNAME"));
        assertEquals(EntryAlreadyInSpaceException.class, dae.getClass());
        assertEquals("UID", ((EntryAlreadyInSpaceException) dae).getUID());
        assertEquals("CLASSNAME", ((EntryAlreadyInSpaceException) dae).getClassName());
    }

    public void testEntryNotInSpaceException() {
        DataAccessException dae = exTranslator.translate(new com.j_spaces.core.client.EntryNotInSpaceException("UID", "SPACENAME", false));
        assertEquals(EntryNotInSpaceException.class, dae.getClass());
        assertEquals("UID", ((EntryNotInSpaceException) dae).getUID());
        assertEquals(false, ((EntryNotInSpaceException) dae).isDeletedByOwnTxn());
    }

    public void testEntryVersionConflictException() {
        DataAccessException dae = exTranslator.translate(new EntryVersionConflictException("UID", 1, 2, "operation"));
        assertEquals(SpaceOptimisticLockingFailureException.class, dae.getClass());
    }

    public void testInvalidFifoClassException() {
        DataAccessException dae = exTranslator.translate(new com.j_spaces.core.InvalidFifoClassException("test", false, true));
        assertEquals(InvalidFifoClassException.class, dae.getClass());
        assertEquals("test", ((InvalidFifoClassException) dae).getClassName());
        assertEquals(true, ((InvalidFifoClassException) dae).isFifoClass());
    }

    public void testInvalidFifoTemplateException() {
        DataAccessException dae = exTranslator.translate(new com.j_spaces.core.InvalidFifoTemplateException("test"));
        assertEquals(InvalidFifoTemplateException.class, dae.getClass());
        assertEquals("test", ((InvalidFifoTemplateException) dae).getTemplateClassName());
    }

    public void testConversionException() {
        DataAccessException dae = exTranslator.translate(new ConversionException("test"));
        assertEquals(ObjectConversionException.class, dae.getClass());
    }

    public void testInternalSpaceExceptionWithIdentifiedException() {
        Exception cause = new Exception("cause");
        net.jini.core.entry.UnusableEntryException uee = new net.jini.core.entry.UnusableEntryException(cause);
        DataAccessException dae = exTranslator.translate(new net.jini.space.InternalSpaceException("test", uee));
        assertEquals(UnusableEntryException.class, dae.getClass());
        assertSame(uee, dae.getCause());
    }

    public void testEntrySerializationException() {
        DataAccessException dae = exTranslator.translate(new com.j_spaces.core.EntrySerializationException("test", new Exception()));
        assertEquals(EntrySerializationException.class, dae.getClass());
    }

    public void testMemoryShortageException() {
        DataAccessException dae = exTranslator.translate(new MemoryShortageException("test1", "test2", "test3", 1, 2));
        assertEquals(SpaceMemoryShortageException.class, dae.getClass());
    }

    public void testOperationTimeoutException() {
        DataAccessException dae = exTranslator.translate(new OperationTimeoutException());
        assertEquals(UpdateOperationTimeoutException.class, dae.getClass());
    }

    public void testUnmatchedTransactionException() {
        DataAccessException dae = exTranslator.translate(new TransactionException("xxx"));
        assertEquals(TransactionDataAccessException.class, dae.getClass());
    }

    public void testInactiveTransactionException() {
        DataAccessException dae = exTranslator.translate(new TransactionException("not active"));
        assertEquals(InactiveTransactionException.class, dae.getClass());
    }

    public void testInvalidTransactionUsageException() {
        DataAccessException dae = exTranslator.translate(new TransactionException("wrong"));
        assertEquals(InvalidTransactionUsageException.class, dae.getClass());
    }
}
