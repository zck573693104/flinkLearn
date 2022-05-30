package com.hycan.bigdata.app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

public class HBaseReader extends RichSourceFunction<Tuple2<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseReader.class);
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private String tableName = "ods_enterprise_ns:t1";

    @Override
    public void open(Configuration parameters) throws Exception {

        conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hbase-site.xml");
        LOG.info("finish create configuration successfully...");

        LOG.info("currentUser: {}", UserGroupInformation.getCurrentUser());
        try {
            conn = ConnectionFactory.createConnection(conf);
            LOG.info("connect success");
        } catch (Exception e) {
            LOG.error("connection failed,", e);
        }
        TableName TABLE_NAME = TableName.valueOf(tableName);
        LOG.info("tableName: " + TABLE_NAME);
        table = conn.getTable(TABLE_NAME);

    }

    private void testSendData(SourceContext<Tuple2<String, String>> ctx) {
        LOG.info("Entering testScanData.");

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;
        try {
            while (true){
                // Instantiate a Get object.
                scan = new Scan();
                scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("icu"));

                // Set the cache size.
                scan.setCaching(100);

                // Submit a scan request.
                rScanner = table.getScanner(scan);

                // Print query results.
                for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                    for (Cell cell : r.rawCells()) {
                        ctx.collect(new Tuple2<String, String>((Bytes.toString(CellUtil.cloneRow(cell))), Bytes.toString(CellUtil.cloneValue(cell))));
                    }
                }
                LOG.info("Scan data successfully.");
            }

        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
        }
        LOG.info("Exiting testScanData.");
    }


    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) {
        testSendData(ctx);
    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            LOG.error("Close HBase Exception:", e.toString());
        }

    }
}
