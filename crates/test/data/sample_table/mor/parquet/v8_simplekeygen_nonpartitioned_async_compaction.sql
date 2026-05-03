/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
DROP TABLE IF EXISTS v8_simplekeygen_nonpartitioned_async_compaction;

CREATE TABLE v8_simplekeygen_nonpartitioned_async_compaction (
    id INT,
    name STRING,
    isActive BOOLEAN,
    shortField SHORT,
    intField INT,
    longField LONG,
    floatField FLOAT,
    doubleField DOUBLE,
    decimalField DECIMAL(10,5),
    dateField DATE,
    timestampField TIMESTAMP,
    binaryField BINARY,
    arrayField ARRAY<STRUCT<arr_struct_f1: STRING, arr_struct_f2: INT>>,
    mapField MAP<STRING, STRUCT<map_field_value_struct_f1: DOUBLE, map_field_value_struct_f2: BOOLEAN>>,
    structField STRUCT<
        field1: STRING,
        field2: INT,
        child_struct: STRUCT<
            child_field1: DOUBLE,
            child_field2: BOOLEAN
        >
    >,
    byteField BYTE
)
USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'id',
    preCombineField = 'longField',
    'hoodie.metadata.enable' = 'true',
    'hoodie.table.version' = '8',
    'hoodie.table.log.file.format' = 'PARQUET',
    'hoodie.logfile.data.block.format' = 'parquet',
    -- Force compaction to be possible after 1 delta commit
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.inline.max.delta.commits' = '1'
)
LOCATION 'file:///tmp/hudi/v8_simplekeygen_nonpartitioned_async_compaction';

-- Create Base File (Commit 1)
INSERT INTO v8_simplekeygen_nonpartitioned_async_compaction VALUES
(1, 'Alice', true, 300, 15000, 1000, 1.0, 3.14, 12345.67, CAST('2023-04-01' AS DATE), CAST('2023-04-01 12:00:00' AS TIMESTAMP), CAST('data' AS BINARY),
 ARRAY(STRUCT('red', 100)), MAP('key1', STRUCT(1.1, true)), STRUCT('Alice', 30, STRUCT(1.1, true)), 10);

-- Create Delta Log (Commit 2)
UPDATE v8_simplekeygen_nonpartitioned_async_compaction SET name = 'Alice_v2', longField = 1001 WHERE id = 1;

-- Schedule Compaction ONLY
-- This leaves the .compaction.requested instant on disk
CALL spark_catalog.system.run_compaction(op => 'schedule', table => 'v8_simplekeygen_nonpartitioned_async_compaction');