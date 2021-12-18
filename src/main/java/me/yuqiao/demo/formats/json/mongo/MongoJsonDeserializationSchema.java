/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.yuqiao.demo.formats.json.mongo;

import me.yuqiao.demo.formats.json.mongo.MongoJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from Mongo JSON to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Mongo's schema definition and can extract the database
 * data and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://github.com/alibaba/MongoShake">Alibaba MongoShake</a>
 */
public final class MongoJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private static final String OP_INSERT = "i";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_CREATE = "c";
    private static final String OP_NOOP = "n";

    /** The deserializer to deserialize Canal JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    /** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Only read changelogs from the specific database. */
    private final @Nullable String namespace;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Number of fields. */
    private final int fieldCount;

    private MongoJsonDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            @Nullable String namespace,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        final RowType jsonRowType = createJsonRowType(physicalDataType, requestedMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used, so it's fine to pass in the produced type
                        // info
                        producedTypeInfo,
                        false, // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        ignoreParseErrors,
                        timestampFormat);
        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters = createMetadataConverters(jsonRowType, requestedMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.namespace = namespace;
        this.ignoreParseErrors = ignoreParseErrors;
        this.fieldCount = ((RowType) physicalDataType.getLogicalType()).getFieldCount();
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link MongoJsonDeserializationSchema}. */
    public static Builder builder(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo) {
        return new Builder(physicalDataType, requestedMetadata, producedTypeInfo);
    }

    /** A builder for creating a {@link MongoJsonDeserializationSchema}. */
    @Internal
    public static final class Builder {
        private final DataType physicalDataType;
        private final List<ReadableMetadata> requestedMetadata;
        private final TypeInformation<RowData> producedTypeInfo;
        private String namespace = null;
        private boolean ignoreParseErrors = false;
        private TimestampFormat timestampFormat = TimestampFormat.SQL;

        private Builder(
                DataType physicalDataType,
                List<ReadableMetadata> requestedMetadata,
                TypeInformation<RowData> producedTypeInfo) {
            this.physicalDataType = physicalDataType;
            this.requestedMetadata = requestedMetadata;
            this.producedTypeInfo = producedTypeInfo;
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public Builder setTimestampFormat(TimestampFormat timestampFormat) {
            this.timestampFormat = timestampFormat;
            return this;
        }

        public MongoJsonDeserializationSchema build() {
            return new MongoJsonDeserializationSchema(
                    physicalDataType,
                    requestedMetadata,
                    producedTypeInfo,
                    namespace,
                    ignoreParseErrors,
                    timestampFormat);
        }
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        try {
            GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            if (namespace != null) {
                String currentDatabase = row.getString(2).toString();
                if (!namespace.equals(currentDatabase)) {
                    return;
                }
            }
            String op = row.getString(1).toString(); // "op" field
            if (OP_INSERT.equals(op)) {
                // "o" field is an array of fields, contains inserted fields
                ArrayData data = row.getArray(0);
                GenericRowData insert = new GenericRowData(data.size());
                for (int i = 0; i < data.size(); i++) {
                    GenericRowData field = (GenericRowData) data.getRow(i, fieldCount);
                    insert.setField(i, field.getField(1));
                }
                insert.setRowKind(RowKind.INSERT);
                emitRow(row, insert, out);
            } else if (OP_UPDATE.equals(op)) {
                // // "data" field is an array of row, contains new rows
                // ArrayData data = row.getArray(0);
                // // "old" field is an array of row, contains old values
                // ArrayData old = row.getArray(1);
                // for (int i = 0; i < data.size(); i++) {
                //     // the underlying JSON deserialization schema always produce GenericRowData.
                //     GenericRowData after = (GenericRowData) data.getRow(i, fieldCount);
                //     GenericRowData before = (GenericRowData) old.getRow(i, fieldCount);
                //     for (int f = 0; f < fieldCount; f++) {
                //         if (before.isNullAt(f)) {
                //             // not null fields in "old" (before) means the fields are changed
                //             // null/empty fields in "old" (before) means the fields are not
                // changed
                //             // so we just copy the not changed fields into before
                //             before.setField(f, after.getField(f));
                //         }
                //     }
                //     before.setRowKind(RowKind.UPDATE_BEFORE);
                //     after.setRowKind(RowKind.UPDATE_AFTER);
                //     emitRow(row, before, out);
                //     emitRow(row, after, out);
                // }
                return;
            } else if (OP_DELETE.equals(op)) {
                // "data" field is an array of row, contains deleted rows
                // ArrayData data = row.getArray(0);
                // for (int i = 0; i < data.size(); i++) {
                //     GenericRowData insert = (GenericRowData) data.getRow(i, fieldCount);
                //     insert.setRowKind(RowKind.DELETE);
                //     emitRow(row, insert, out);
                // }
                return;
            } else if (OP_CREATE.equals(op)) {
                // "data" field is null and "op" is "CREATE" which means
                // this is a DDL change event, and we should skip it.
                return;
            } else if (OP_NOOP.equals(op)) {
                // no op,and we should skip it.
                return;
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                            format(
                                    "Unknown \"op\" value \"%s\". The Mongo JSON message is '%s'",
                                    op, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt Mongo JSON message '%s'.", new String(message)), t);
            }
        }
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }
        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
        }
        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoJsonDeserializationSchema that = (MongoJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && Objects.equals(namespace, that.namespace)
                && ignoreParseErrors == that.ignoreParseErrors
                && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer,
                hasMetadata,
                producedTypeInfo,
                namespace,
                ignoreParseErrors,
                fieldCount);
    }

    // --------------------------------------------------------------------------------------------

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("o", DataTypes.ARRAY(physicalDataType)),
                        DataTypes.FIELD("op", DataTypes.STRING()),
                        ReadableMetadata.NAMESPACE.requiredJsonField);
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .filter(m -> m != ReadableMetadata.NAMESPACE)
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType jsonRowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(m -> convert(jsonRowType, m))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convert(RowType jsonRowType, ReadableMetadata metadata) {
        final int pos = jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(GenericRowData root, int unused) {
                return metadata.converter.convert(root, pos);
            }
        };
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row that comes out of the JSON schema and
     * converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
