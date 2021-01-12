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

import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MongoJsonSerializationSchema} and {@link MongoJsonDeserializationSchema}.
 */
public class MongoJsonSerDeSchemaTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final DataType PHYSICAL_DATA_TYPE =
            ROW(
                    FIELD("Name", STRING().notNull()),
                    FIELD("Value", STRING()));

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = readLines("mongo-data-filter-table.txt");
        MongoJsonDeserializationSchema deserializationSchema =
                MongoJsonDeserializationSchema.builder(
                        PHYSICAL_DATA_TYPE,
                        Collections.emptyList(),
                        InternalTypeInfo.of(PHYSICAL_DATA_TYPE.getLogicalType()))
                        .setNamespace("mydb.product")
                        .build();
        runTest(lines, deserializationSchema);
    }

    public void runTest(List<String> lines, MongoJsonDeserializationSchema deserializationSchema)
            throws Exception {
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        List<String> expected =
                Arrays.asList(
                        "+I(1,123456789)");
        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        // test Serialization
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static List<String> readLines(String resource) throws IOException {
        final URL url = MongoJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
