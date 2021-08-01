package club.projectgaia.bigdata.homework3;


import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-08-01
 */
public class HBaseClient {
    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private static Configuration config = HBaseConfiguration.create();
    private static Connection connection;
    private static Admin admin;
    private static Table table;
    private static final TableName TABLE_NAME = TableName.valueOf("luoxiaolong:student");

    private static final byte[] CF_NAME = Bytes.toBytes("name");
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_SCORE = Bytes.toBytes("score");

    private static final byte[] COLUMN_NAME = Bytes.toBytes("name");
    private static final byte[] COLUMN_ID = Bytes.toBytes("student_id");
    private static final byte[] COLUMN_CLASS = Bytes.toBytes("class");
    private static final byte[] COLUMN_UNDERSTANDING = Bytes.toBytes("understanding");
    private static final byte[] COLUMN_PROGRAMMING = Bytes.toBytes("programming");

    public static void main(String[] args) {
        try {
            init();
            createTable();
            insert();
            search();
        } catch (Exception e) {
            logger.error("has error!", e);
        }

    }

    public static void search() throws Exception {
        Scan scan = new Scan();
        Filter rowFilter = new PrefixFilter(Bytes.toBytes("G202005790"));
        scan.setFilter(rowFilter);
        scan.setReversed(true);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            logger.info("row : {}", Bytes.toString(result.getRow()));
            result.getNoVersionMap().forEach((family, column) -> {
                logger.info("family :{} ", Bytes.toString(family));
                column.forEach((key, value) -> {
                    logger.info("{} ----- {} ", Bytes.toString(key), Bytes.toString(value));
                });
            });
        }
        scanner.close();
    }

    public static void insert() throws Exception {
        Put tom = new Put(Bytes.toBytes("G20200579010831"));
        tom.addColumn(CF_NAME, COLUMN_NAME, Bytes.toBytes("tome"));
        tom.addColumn(CF_INFO, COLUMN_CLASS, Bytes.toBytes("1"));
        tom.addColumn(CF_INFO, COLUMN_ID, Bytes.toBytes("G20200579010831"));
        tom.addColumn(CF_SCORE, COLUMN_UNDERSTANDING, Bytes.toBytes("75"));
        tom.addColumn(CF_SCORE, COLUMN_PROGRAMMING, Bytes.toBytes("82"));
        table.put(tom);
        logger.info("create tome");

        Put jerry = new Put(Bytes.toBytes("G20200579010832"));
        jerry.addColumn(CF_NAME, COLUMN_NAME, Bytes.toBytes("jerry"));
        jerry.addColumn(CF_INFO, COLUMN_CLASS, Bytes.toBytes("1"));
        jerry.addColumn(CF_INFO, COLUMN_ID, Bytes.toBytes("G20200579010832"));
        jerry.addColumn(CF_SCORE, COLUMN_UNDERSTANDING, Bytes.toBytes("85"));
        jerry.addColumn(CF_SCORE, COLUMN_PROGRAMMING, Bytes.toBytes("67"));
        table.put(jerry);
        logger.info("create jerry");

        Put jack = new Put(Bytes.toBytes("G20200579010833"));
        jack.addColumn(CF_NAME, COLUMN_NAME, Bytes.toBytes("jack"));
        jack.addColumn(CF_INFO, COLUMN_CLASS, Bytes.toBytes("2"));
        jack.addColumn(CF_INFO, COLUMN_ID, Bytes.toBytes("G20200579010833"));
        jack.addColumn(CF_SCORE, COLUMN_UNDERSTANDING, Bytes.toBytes("80"));
        jack.addColumn(CF_SCORE, COLUMN_PROGRAMMING, Bytes.toBytes("80"));
        table.put(jack);
        logger.info("create jack");

        Put phoenix = new Put(Bytes.toBytes("G20200579010834"));
        phoenix.addColumn(CF_NAME, COLUMN_NAME, Bytes.toBytes("phoenix"));
        phoenix.addColumn(CF_INFO, COLUMN_CLASS, Bytes.toBytes("2"));
        phoenix.addColumn(CF_INFO, COLUMN_ID, Bytes.toBytes("G20200579010834"));
        phoenix.addColumn(CF_SCORE, COLUMN_UNDERSTANDING, Bytes.toBytes("90"));
        phoenix.addColumn(CF_SCORE, COLUMN_PROGRAMMING, Bytes.toBytes("90"));
        table.put(phoenix);
        logger.info("create phoenix");

    }

    public static void createTable() throws Exception {
        if (admin.tableExists(TABLE_NAME)) {
            logger.info("table always exists");
        } else {
            NamespaceDescriptor myNs = NamespaceDescriptor.create("luoxiaolong").build();
            admin.createNamespace(myNs);
            logger.info("create namespace luoxiaolong");

            TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(TABLE_NAME);
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor nameFamily =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(CF_NAME).setMaxVersions(3);
            tableDescriptor.setColumnFamily(nameFamily);
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor infoFamily =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(CF_INFO).setMaxVersions(3);
            tableDescriptor.setColumnFamily(infoFamily);
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor scoreFamily =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(CF_SCORE).setMaxVersions(3);
            tableDescriptor.setColumnFamily(scoreFamily);
            admin.createTable(tableDescriptor);
            logger.info("create table {} success", TABLE_NAME.getNameAsString());
        }
        table = connection.getTable(TABLE_NAME);
    }

    public static void close() throws Exception {
        table.close();
        admin.close();
        connection.close();
    }

    public static void init() throws Exception {
        config.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02,jikehadoop03");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();

    }
}
