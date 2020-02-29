/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HbaseLocalClusterIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalClusterIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    private static HbaseLocalCluster hbaseLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
            .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
            .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
            .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
            .build();
        zookeeperLocalCluster.start();

        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .build();
        hbaseLocalCluster.start();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseLocalCluster.stop();
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testHbaseLocalCluster() throws Exception {

        String tableName = propertyParser.getProperty(ConfigVars.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_TEST_NUM_ROWS_TO_PUT_KEY));
        Configuration configuration = hbaseLocalCluster.getHbaseConfiguration();

        try (final Connection connection = ConnectionFactory.createConnection(configuration)) {
            LOG.info("HBASE: Deleting table {}", tableName);
            deleteHbaseTable(tableName, connection);

            LOG.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
            createHbaseTable(tableName, colFamName, connection);

            LOG.info("HBASE: Populate the table with {} rows.", numRowsToPut);
            for (int i = 0; i < numRowsToPut; i++) {
                putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, connection);
            }


            LOG.info("HBASE: Fetching and comparing the results");
            for (int i = 0; i < numRowsToPut; i++) {
                Result result = getRow(tableName, colFamName, String.valueOf(i), colQualiferName, connection);
                assertEquals("row_" + i, new String(result.value()));
            }
        }

    }

    private static void createHbaseTable(String tableName, String colFamily,
                                         Connection connection) throws Exception {

        final Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);

        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }

    private static void deleteHbaseTable(String table, Connection connection) throws Exception {

        final Admin admin = connection.getAdmin();
        final TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private static void putRow(String tableName, String colFamName, String rowKey, String colQualifier, String value,
                               Connection connection) throws Exception {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value));
            table.put(put);
        }
    }

    private static Result getRow(String tableName, String colFamName, String rowKey, String colQualifier,
                                 Connection connection) throws Exception {
        Result result;
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier));
            get.readVersions(1);
            result = table.get(get);
            return result;
        }
    }

}
