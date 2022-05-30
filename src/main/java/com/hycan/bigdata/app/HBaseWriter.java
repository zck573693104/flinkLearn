package com.hycan.bigdata.app;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

public class HBaseWriter<T> implements OutputFormat<Tuple2<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);

    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;
    private String tableName = "sometwo";

    @Override
    public void configure(Configuration configuration) {

        conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hbase-site.xml");
        LOG.info("finish create configuration successfully...");
    }

    @Override
    public void open(int i, int i1) throws IOException {

        LOG.info("currentUser: {}", UserGroupInformation.getCurrentUser());
        try {
            conn = ConnectionFactory.createConnection(conf);
            LOG.info("connect success");
        } catch (Exception e) {
            LOG.error("connection failed,", e);
        }

    }

    @Override
    public void writeRecord(Tuple2<String, String> data) throws IOException {
        TableName TABLE_NAME = TableName.valueOf(tableName);
        LOG.info("tableName: " + TABLE_NAME);
        table = conn.getTable(TABLE_NAME);
        testCreateTable(TABLE_NAME);
        testPut(data);
    }

    private void testCreateTable(TableName tn) {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        HTableDescriptor htd = new HTableDescriptor(tn);

        // Set the column family name to info.
        HColumnDescriptor hcd = new HColumnDescriptor("info");
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);
        htd.addFamily(hcd);
        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tn)) {
                LOG.info("Creating table...");
                admin.createTable(htd);
                LOG.info(admin.getClusterStatus().toString());
                LOG.info(admin.listNamespaceDescriptors().toString());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }

    public void testPut(Tuple2<String,String> data) {
        LOG.info("Entering testPut.");
        byte[] familyName = Bytes.toBytes("info");
        try {
            Put put = new Put(Bytes.toBytes(data._1));
            put.addColumn(familyName, Bytes.toBytes("name"), Bytes.toBytes(data._2));
            table.put(put);

            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        }
        LOG.info("Exiting testPut.");
    }


    @Override
    public void close() throws IOException {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            LOG.error("Close HBase Exception:", e);
        }
    }
}
