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

package org.openspaces.persistency.cassandra;

import com.gigaspaces.document.SpaceDocument;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.cassandra.datasource.SpaceDocumentMapper;
import org.openspaces.persistency.cassandra.error.SpaceCassandraSchemaUpdateException;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadataCache;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadataMetadata;
import org.openspaces.persistency.cassandra.meta.TypedColumnMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.meta.types.SerializerProvider;
import org.openspaces.persistency.cassandra.meta.types.ValidatorClassInferer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchSizeHint;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.MappedColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * A wrapper around the Cassandra Hector client library.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class HectorCassandraClient {

    private static final int SLEEP_BEFORE_RETRY = 1000 * 11;

    private static final Log logger = LogFactory.getLog(HectorCassandraClient.class);

    private final NamedLockProvider namedLock = new NamedLockProvider();
    private final ConcurrentMap<String, ColumnFamilyTemplate<Object, String>> templates = new ConcurrentHashMap<String, ColumnFamilyTemplate<Object, String>>();
    private final ConcurrentMap<String, SpaceDocumentMapper> mapperTemplates = new ConcurrentHashMap<String, SpaceDocumentMapper>();
    private final ColumnFamilyMetadataCache metadataCache = new ColumnFamilyMetadataCache();

    private final Integer columnFamilyGcGraceSeconds;
    private final Keyspace keyspace;
    private final Cluster cluster;

    private final Object lock = new Object();
    private boolean closed = false;

    private final CassandraConsistencyLevel readConsistencyLevel;
    private final CassandraConsistencyLevel writeConsistencyLevel;

    /**
     * @see HectorCassandraClientConfigurer#create()
     */
    public HectorCassandraClient(
            final CassandraHostConfigurator config,
            final String keyspaceName,
            final String clusterName,
            final Integer columnFamilyGcGraceSeconds,
            final CassandraConsistencyLevel readConsistencyLevel,
            final CassandraConsistencyLevel writeConsistencyLevel) {

        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        if (keyspaceName == null) {
            throw new IllegalArgumentException("keyspacename must be set");
        }

        if (columnFamilyGcGraceSeconds != null && columnFamilyGcGraceSeconds < 0) {
            throw new IllegalArgumentException("columnFamilyGcGraceSeconds must be a non negative");
        }

        this.columnFamilyGcGraceSeconds = columnFamilyGcGraceSeconds;

        if (readConsistencyLevel == CassandraConsistencyLevel.ANY) {
            throw new IllegalArgumentException(readConsistencyLevel + "consistency is not supported as read " +
                    " consistency level");
        }

        CassandraConsistencyLevel actualReadConsistencyLevel = readConsistencyLevel;
        if (actualReadConsistencyLevel == null) {
            actualReadConsistencyLevel = CassandraConsistencyLevel.QUORUM;
        }

        CassandraConsistencyLevel actualWriteConsistencyLevel = writeConsistencyLevel;
        if (actualWriteConsistencyLevel == null) {
            actualWriteConsistencyLevel = CassandraConsistencyLevel.QUORUM;
        }

        this.readConsistencyLevel = actualReadConsistencyLevel;
        this.writeConsistencyLevel = actualWriteConsistencyLevel;

        String hectorClusterName = clusterName;
        if (hectorClusterName == null) {
            hectorClusterName = "cluster";
        }
        cluster = HFactory.getOrCreateCluster(hectorClusterName, config);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster, createConsistencyLevelPolicy());
        validateKeyspaceExists();
    }

    private ConsistencyLevelPolicy createConsistencyLevelPolicy() {
        ConfigurableConsistencyLevel policy = new ConfigurableConsistencyLevel();
        Map<String, HConsistencyLevel> mappping = new HashMap<String, HConsistencyLevel>();
        mappping.put(ColumnFamilyMetadataMetadata.NAME, HConsistencyLevel.QUORUM);
        policy.setReadCfConsistencyLevels(mappping);
        policy.setWriteCfConsistencyLevels(mappping);
        policy.setDefaultReadConsistencyLevel(HConsistencyLevel.valueOf(readConsistencyLevel.name()));
        policy.setDefaultWriteConsistencyLevel(HConsistencyLevel.valueOf(writeConsistencyLevel.name()));
        return policy;
    }

    /**
     * Closes hector's connection pool.
     */
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (cluster != null) {
                HFactory.shutdownCluster(cluster);
            }
            closed = true;
        }
    }

    private void validateKeyspaceExists() {
        KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keyspace.getKeyspaceName());
        if (keyspaceDefinition == null) {
            throw new IllegalArgumentException("Keyspace: " + keyspace.getKeyspaceName() + " does not exits.");
        }
    }

    private ColumnFamilyDefinition getColumnFamilyDefinition(ColumnFamilyMetadata metadata) {
        KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keyspace.getKeyspaceName());
        List<ColumnFamilyDefinition> cfDefs = keyspaceDefinition.getCfDefs();
        for (ColumnFamilyDefinition columnFamilyDefinition : cfDefs) {
            if (columnFamilyDefinition.getName().equals(metadata.getColumnFamilyName())) {
                return columnFamilyDefinition;
            }
        }

        return null;
    }

    private boolean isColumnFamilyExists(ColumnFamilyMetadata metadata) {
        return getColumnFamilyDefinition(metadata) != null;
    }

    public void createMetadataColumnFamilyColumnFamilyIfNecessary() {
        createColumnFamilyIfNecessary(ColumnFamilyMetadataMetadata.INSTANCE, true);
    }

    /**
     * Writes the given {@link ColumnFamilyRow}'s in a mutate operation to the configured keyspace.
     *
     * @param rows The {@link ColumnFamilyRow}'s to perform the mutate operation on.
     *
     *             Contract: It is assumed that all the rows in the passed list argument belong to
     *             the same column family. Failure to comply with contract will most likely lead to
     *             some data corruption.
     */
    public void performBatchOperation(List<ColumnFamilyRow> rows) {
        if (rows.isEmpty()) {
            return;
        }

        // rows is not empty and the contract says that `rows` should all belong to the same column family.
        Serializer<?> keySerializer = rows.iterator().next().getColumnFamilyMetadata().getKeySerializer();
        Mutator<Object> mutator = createMutator(rows.size(), keySerializer);

        for (ColumnFamilyRow row : rows) {
            switch (row.getRowType()) {
                case Update: {
                    // for correctness we delete the entire row first - comes at performance cost
                    // implementing support for in place update would be a MUCH more efficient
                    // way of doing updates
                    mutator.addDeletion(row.getKeyValue(),
                            row.getColumnFamilyMetadata().getColumnFamilyName());
                    // FALLTHROUGH (no break!)
                }
                case PartialUpdate:
                case Write: {
                    for (ColumnData columnData : row.getColumns().values()) {
                        if (columnData.getValue() == null) {
                            continue;
                        }

                        mutator.addInsertion(row.getKeyValue(),
                                row.getColumnFamilyMetadata().getColumnFamilyName(),
                                HFactory.createColumn(columnData.getColumnMetadata().getFullName(),
                                        columnData.getValue(),
                                        StringSerializer.get(),
                                        columnData.getColumnMetadata().getSerializer()));

                    }
                    break;
                }
                case Remove: {
                    mutator.addDeletion(row.getKeyValue(),
                            row.getColumnFamilyMetadata().getColumnFamilyName());
                    break;
                }
                default:
                    throw new IllegalStateException("should not have gotten here, got: " + row.getRowType());
            }
        }

        mutator.execute();
    }

    @SuppressWarnings("unchecked")
    private Mutator<Object> createMutator(int numberOfRowsHint, Serializer<?> keySerializer) {
        return HFactory.createMutator(keyspace,
                (Serializer<Object>) keySerializer,
                new BatchSizeHint(numberOfRowsHint, 15));
    }

    /**
     * Creates a column family on the configured keyspace if one does not already exist.
     *
     * @param metadata      The metadata describing the column family to create.
     * @param shouldPersist Should the {@link ColumnFamilyMetadata} instance be persisted to the
     *                      internal metadata column family.
     */
    public void createColumnFamilyIfNecessary(ColumnFamilyMetadata metadata, boolean shouldPersist) {

        ReentrantLock lockForType = namedLock.forName(metadata.getTypeName());
        lockForType.lock();
        try {

            final boolean columnFamilyExists = isColumnFamilyExists(metadata);

            if (columnFamilyExists) {
                if (metadata != ColumnFamilyMetadataMetadata.INSTANCE) {
                    metadataCache.addColumnFamilyMetadata(metadata.getTypeName(), metadata);
                    // we persist the metadata again here, because it is possible
                    // that the table creation was successful but writing the internal metadata
                    // failed. without this, we will never write the internal metadata.
                    // the assumption here is that we write the exact same metadata.
                    if (shouldPersist) {
                        persistColumnFamilyMetadata(metadata);
                    }
                }
                return;
            }

            ThriftCfDef cfDef = (ThriftCfDef) HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(),
                    metadata.getColumnFamilyName());
            cfDef.setColumnMetadata(new ArrayList<ColumnDefinition>());
            cfDef.setDefaultValidationClass(ValidatorClassInferer.getBytesTypeValidationClass());
            cfDef.setComparatorType(StringSerializer.get().getComparatorType());
            cfDef.setKeyAlias(StringSerializer.get().toByteBuffer(metadata.getKeyName()));
            cfDef.setKeyValidationClass(ValidatorClassInferer.infer(metadata.getKeyType()));
            cfDef.setComment(metadata.getTypeName());

            if (columnFamilyGcGraceSeconds != null) {
                cfDef.setGcGraceSeconds(columnFamilyGcGraceSeconds);
            }

            for (TypedColumnMetadata columnMetadata : metadata.getColumns().values()) {
                String validationClass = ValidatorClassInferer.infer(columnMetadata.getType());
                addColumnDefinitionToColumnFamilyDefinition(metadata, cfDef, columnMetadata.getFullName(), validationClass);
            }

            // create stub columns for statically defined indexes of dynamic properties
            for (String index : metadata.getIndexes()) {
                // we took care of these in the previous for loop
                if (metadata.getColumns().containsKey(index)) {
                    continue;
                }

                String validationClass = ValidatorClassInferer.getBytesTypeValidationClass();
                addColumnDefinitionToColumnFamilyDefinition(metadata, cfDef, index, validationClass);
            }

            if (logger.isInfoEnabled()) {
                logger.info("Creating column family: " + metadata);
            }

            try {
                // first create the actual column family
                try {
                    cluster.addColumnFamily(cfDef, true);
                } catch (Exception e) {
                    // This could be due to current type introduction
                    // If column family already exists, we can ignore this exception, otherwise we propage
                    if (logger.isInfoEnabled()) {
                        logger.info("Column family creation failed, " +
                                "waiting " + (SLEEP_BEFORE_RETRY / 1000) + " seconds and then testing to see whether " +
                                "the column family was already created.", e);
                    }
                    Thread.sleep(SLEEP_BEFORE_RETRY);
                    if (!isColumnFamilyExists(metadata)) {
                        throw e;
                    }
                }

                if (metadata != ColumnFamilyMetadataMetadata.INSTANCE) {
                    metadataCache.addColumnFamilyMetadata(metadata.getTypeName(), metadata);
                    if (shouldPersist) {
                        persistColumnFamilyMetadata(metadata);
                    }
                }

            } catch (Exception e) {
                throw new SpaceCassandraSchemaUpdateException("Failed adding column family definition to cassandra", e, true);
            }
        } finally {
            lockForType.unlock();
        }
    }

    /**
     * Adds a secondary index to the provided columns in the column family matching the provided
     * typeName. Creates the column if it does not already exist.
     *
     * @param typeName    The type name describing the matchin column family.
     * @param columnNames the columns to which secondary indexes should be added.
     */
    public void addIndexesToColumnFamily(
            String typeName,
            List<String> columnNames,
            SpaceDocumentColumnFamilyMapper mapper) {
        ColumnFamilyMetadata metadata = metadataCache.getColumnFamilyMetadata(typeName);
        if (metadata == null) {
            metadata = fetchColumnFamilyMetadata(typeName, mapper);
            if (metadata == null) {
                throw new SpaceCassandraSchemaUpdateException("Failed finding column family metadata for " +
                        typeName, null, false);
            }
        }

        ReentrantLock lockForType = namedLock.forName(metadata.getTypeName());
        lockForType.lock();
        try {
            metadata.getIndexes().addAll(columnNames);

            ColumnFamilyDefinition columnFamilyDefinition = getColumnFamilyDefinition(metadata);
            if (columnFamilyDefinition == null) {
                throw new SpaceCassandraSchemaUpdateException("column family definition: " + metadata.getColumnFamilyName() +
                        " for type: " + typeName + " not found", null, false);
            }

            ThriftCfDef thriftCfDef = new ThriftCfDef(columnFamilyDefinition);
            for (String columnName : columnNames) {

                ByteBuffer serializedColumnName = StringSerializer.get().toByteBuffer(columnName);
                ColumnDefinition originalColumnDefinition = getColumnDefinition(serializedColumnName, thriftCfDef);

                if (originalColumnDefinition == null) {
                    String validationClass = ValidatorClassInferer.getBytesTypeValidationClass();
                    addColumnDefinitionToColumnFamilyDefinition(metadata, thriftCfDef, columnName, validationClass);
                } else if (originalColumnDefinition.getIndexName() != null) {
                    // index already exists for this column, no need to proceed
                    continue;
                } else {
                    List<ColumnDefinition> currentColumns = thriftCfDef.getColumnMetadata();
                    thriftCfDef.setColumnMetadata(new ArrayList<ColumnDefinition>());
                    BasicColumnDefinition replacedColumnDefinition = new BasicColumnDefinition();
                    replacedColumnDefinition.setName(originalColumnDefinition.getName());
                    replacedColumnDefinition.setValidationClass(originalColumnDefinition.getValidationClass());
                    replacedColumnDefinition.setIndexName(generateIndexName(typeName, columnName));
                    replacedColumnDefinition.setIndexType(ColumnIndexType.KEYS);

                    for (ColumnDefinition columnDef : currentColumns) {
                        if (columnDef != originalColumnDefinition) {
                            thriftCfDef.addColumnDefinition(columnDef);
                        } else {
                            thriftCfDef.addColumnDefinition(replacedColumnDefinition);
                        }
                    }
                }
            }

            if (logger.isInfoEnabled()) {
                logger.info("Adding indexes to columns: " + columnNames + " of type: " +
                        metadata.getTypeName() + ", column family: " + metadata.getColumnFamilyName());
            }

            try {
                cluster.updateColumnFamily(thriftCfDef, true);
            } catch (Exception e) {
                throw new SpaceCassandraSchemaUpdateException("Failed adding column family definition to cassandra", null, true);
            }

        } finally {
            lockForType.unlock();
        }
    }

    private ColumnDefinition getColumnDefinition(ByteBuffer serializedColumnName, ThriftCfDef thriftCfDef) {
        for (ColumnDefinition columnDef : thriftCfDef.getColumnMetadata()) {
            if (serializedColumnName.equals(columnDef.getName())) {
                return columnDef;
            }
        }
        return null;
    }

    private void addColumnDefinitionToColumnFamilyDefinition(
            ColumnFamilyMetadata metadata,
            ThriftCfDef columnFamilyDefinition,
            String columnFullName,
            String validationClass) {
        BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
        ByteBuffer serializedName = StringSerializer.get().toByteBuffer(columnFullName);

        columnDefinition.setName(serializedName);
        columnDefinition.setValidationClass(validationClass);
        if (metadata.getIndexes().contains(columnFullName)) {
            columnDefinition.setIndexName(generateIndexName(metadata.getTypeName(),
                    columnFullName));
            columnDefinition.setIndexType(ColumnIndexType.KEYS);
        }

        columnFamilyDefinition.addColumnDefinition(columnDefinition);
    }

    private static String generateIndexName(String typeName, String columnName) {
        return (typeName + "_" + columnName).replace(".", "_");
    }

    private void persistColumnFamilyMetadata(ColumnFamilyMetadata metadata) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding metadata for type " + metadata.getTypeName() + " to internal metadata column family");
        }

        try {
            ColumnFamilyTemplate<Object, String> template = getTemplate(ColumnFamilyMetadataMetadata.INSTANCE);
            ColumnFamilyUpdater<Object, String> updater = template.createUpdater(metadata.getTypeName());
            updater.setByteBuffer(ColumnFamilyMetadataMetadata.BLOB_COLUMN_NAME,
                    SerializerProvider.getObjectSerializer().toByteBuffer(metadata));
            template.update(updater);
        } catch (Exception e) {
            throw new SpaceCassandraSchemaUpdateException("Failed persisting column family metadata for " +
                    metadata.getTypeName(), e, true);
        }
    }

    /**
     * Tries to read from the internal metadata column family, the {@link ColumnFamilyMetadata}
     * metadata matching the given type name. If found, this metadata is stored in cache and can
     * later be aquired by calling {@link #getColumnFamilyMetadata(String)}
     *
     * @param typeName The typeName describing the matching column family.
     * @return The {@link ColumnFamilyMetadata} instance if found, null otherwise.
     */
    public ColumnFamilyMetadata fetchColumnFamilyMetadata(String typeName, SpaceDocumentColumnFamilyMapper mapper) {
        ColumnFamilyTemplate<Object, String> template = getTemplate(ColumnFamilyMetadataMetadata.INSTANCE);
        if (!template.isColumnsExist(typeName)) {
            return null;
        }

        ColumnFamilyResult<Object, String> result = template.queryColumns(typeName);
        HColumn<String, ByteBuffer> column = result.getColumn(ColumnFamilyMetadataMetadata.BLOB_COLUMN_NAME);
        ColumnFamilyMetadata newMetadata = (ColumnFamilyMetadata) SerializerProvider.getObjectSerializer()
                .fromByteBuffer(column.getValueBytes());
        initMetadataAndAddToCache(mapper, newMetadata);
        return newMetadata;
    }

    /**
     * Reads all the column families metadata for the internal metadata column family into cache.
     *
     * @return A {@link java.util.Map} from type name to its matching {@link ColumnFamilyMetadata}.
     * Of all the currently known column families.
     */
    public Map<String, ColumnFamilyMetadata> populateColumnFamiliesMetadata(SpaceDocumentColumnFamilyMapper mapper) {
        RangeSlicesQuery<String, String, Object> rangeQuery = HFactory.createRangeSlicesQuery(keyspace,
                StringSerializer.get(),
                StringSerializer.get(),
                SerializerProvider.getObjectSerializer())
                .setColumnFamily(ColumnFamilyMetadataMetadata.NAME)
                .setColumnNames(ColumnFamilyMetadataMetadata.BLOB_COLUMN_NAME)
                .setKeys("", "");

        OrderedRows<String, String, Object> result = rangeQuery.execute().get();
        for (Row<String, String, Object> row : result) {
            HColumn<String, Object> column =
                    row.getColumnSlice()
                            .getColumnByName(ColumnFamilyMetadataMetadata.BLOB_COLUMN_NAME);
            ColumnFamilyMetadata metadata = (ColumnFamilyMetadata) column.getValue();
            initMetadataAndAddToCache(mapper, metadata);
        }

        return metadataCache.getColumnFamiliesMetadata();
    }

    private void initMetadataAndAddToCache(SpaceDocumentColumnFamilyMapper mapper,
                                           ColumnFamilyMetadata metadata) {
        Serializer<Object> fixedPropertyValueSerializer = mapper.getTypeNodeIntrospector()
                .getFixedPropertyValueSerializer();
        if (fixedPropertyValueSerializer != null) {
            metadata.setFixedPropertySerializerForTypedColumn(fixedPropertyValueSerializer);
        }
        metadataCache.addColumnFamilyMetadata(metadata.getTypeName(), metadata);
    }

    /**
     * Reads the entry matching the typeName and key value from the matching column family.
     *
     * @param typeName The typeName describing the matching column family.
     * @param keyValue The key of the requested entry.
     * @return The SpaceDocument matching the key if found, null otherwise.
     */
    public SpaceDocument readDocmentByKey(
            SpaceDocumentColumnFamilyMapper mapper,
            String typeName,
            Object keyValue) {
        ColumnFamilyMetadata metadata = metadataCache.getColumnFamilyMetadata(typeName);
        if (metadata == null) {
            metadata = fetchColumnFamilyMetadata(typeName, mapper);

            if (metadata == null) {
                return null;
            }
        }

        ColumnFamilyTemplate<Object, String> template = getTemplate(metadata);

        if (!template.isColumnsExist(keyValue)) {
            return null;
        }

        SpaceDocumentMapper hectorMapper = getMapperTemplate(metadata, mapper);
        try {
            return template.queryColumns(keyValue, hectorMapper);
        } catch (Exception e) {
            // entry may have been removed in the meantime
            return null;
        }
    }

    /**
     * Reads the entries matching the typeName and key values from the matching column family.
     *
     * @param typeName  The typeName describing the matching column family.
     * @param keyValues The keys of the requested entry.
     * @return A map from key to SpaceDocument, including found entries.
     */
    public Map<Object, SpaceDocument> readDocumentsByKeys(
            SpaceDocumentColumnFamilyMapper mapper,
            String typeName,
            Object[] keyValues) {
        Map<Object, SpaceDocument> result = Collections.emptyMap();
        ColumnFamilyMetadata metadata = metadataCache.getColumnFamilyMetadata(typeName);
        if (metadata == null) {
            metadata = fetchColumnFamilyMetadata(typeName, mapper);
            if (metadata == null) {
                return result;
            }
        }

        ColumnFamilyTemplate<Object, String> template = getTemplate(metadata);
        SpaceDocumentMapper hectorMapper = getMapperTemplate(metadata, mapper);
        MappedColumnFamilyResult<Object, String, SpaceDocument> queryResult =
                template.queryColumns(Arrays.asList(keyValues), hectorMapper);

        for (int i = 0; i < keyValues.length; i++) {
            if (queryResult.hasResults()) {
                if (result.isEmpty()) {
                    result = new HashMap<Object, SpaceDocument>();
                }
                result.put(queryResult.getKey(), queryResult.getRow());
            }
            if (i < keyValues.length - 1) {
                queryResult.next();
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private ColumnFamilyTemplate<Object, String> getTemplate(ColumnFamilyMetadata metadata) {
        ColumnFamilyTemplate<Object, String> template = templates.get(metadata.getTypeName());
        if (template == null) {
            template = new ThriftColumnFamilyTemplate<Object, String>(keyspace,
                    metadata.getColumnFamilyName(),
                    (Serializer<Object>) metadata.getKeySerializer(),
                    StringSerializer.get());
            templates.put(metadata.getTypeName(), template);
        }
        return template;
    }

    private SpaceDocumentMapper getMapperTemplate(
            ColumnFamilyMetadata metadata,
            SpaceDocumentColumnFamilyMapper mapper) {
        SpaceDocumentMapper mapperTemplate = mapperTemplates.get(metadata.getTypeName());
        if (mapperTemplate == null) {
            mapperTemplate = new SpaceDocumentMapper(metadata, mapper);
            mapperTemplates.put(metadata.getTypeName(), mapperTemplate);
        }

        return mapperTemplate;
    }

    /**
     * Reads from cache the {@link ColumnFamilyMetadata} instance matching the provided typeName
     *
     * @param typeName The typeName describing the matching column family.
     * @return The {@link ColumnFamilyMetadata} instance if found, null otherwise.
     */
    public ColumnFamilyMetadata getColumnFamilyMetadata(String typeName) {
        return metadataCache.getColumnFamilyMetadata(typeName);
    }

    /**
     * @return All currently available in cache column families metadata.
     */
    public Map<String, ColumnFamilyMetadata> getColumnFamiliesMetadata() {
        return metadataCache.getColumnFamiliesMetadata();
    }

    /**
     * @return the readConsistencyLevel
     */
    public CassandraConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    /**
     * @return the writeConsistencyLevel
     */
    public CassandraConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }
}
