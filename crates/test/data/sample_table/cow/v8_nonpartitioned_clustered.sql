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
CREATE TABLE v8_nonpartitioned_clustered (
    id INT,
    name STRING,
    ts LONG,
    isActive BOOLEAN,
    price DECIMAL(10,2)
)
USING HUDI
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'ts',
    'hoodie.table.version' = '8',
    'hoodie.metadata.enable' = 'true'
)
LOCATION 'file:///tmp/hudi/v8_nonpartitioned_clustered';

-- Insert Initial Batch
INSERT INTO v8_nonpartitioned_clustered VALUES 
(1, 'Initial', 1000, true, 19.99),
(2, 'Second', 2000, false, 25.50);

-- Trigger Clustering via a second insert with specific options
-- In Spark SQL shell, we can trigger clustering manually after the insert
INSERT INTO v8_nonpartitioned_clustered VALUES 
(3, 'Clustered_Entry', 3000, true, 45.00);

-- Manual trigger to ensure the .replacecommit file is generated for testing
CALL run_clustering(table => 'v8_nonpartitioned_clustered');