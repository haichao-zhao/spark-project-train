package com.zhc.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.NavigableMap;

public class HbaseAppTest {

    Connection conn = null;
    Table table = null;
    Admin admin = null;

    String tableName = "zhc_hbase_java_api";

    @Before
    public void setup() throws Exception {

        Configuration conf = new Configuration();
        conf.set("hbase.rootdir", "hdfs://localhost:8020/hbase");
        conf.set("hbase.zookeeper.quorum", "localhost:2181");

        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();

        Assert.assertNotNull(conn);
        Assert.assertNotNull(admin);

    }

    @Test
    public void getConnection() {

    }

    @Test
    public void createTable() throws Exception {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            System.out.println(tableName + ": 已经存在");
        } else {
            HTableDescriptor desc = new HTableDescriptor(table);
            desc.addFamily(new HColumnDescriptor("info"));
            desc.addFamily(new HColumnDescriptor("address"));
            admin.createTable(desc);

            System.out.println(tableName + ": 创建成功");
        }
    }


    @Test
    public void queryTableInfo() throws Exception {

        HTableDescriptor[] tables = admin.listTables();

        for (HTableDescriptor descriptor : tables) {
            String name = descriptor.getNameAsString();
            System.out.println(name);

            HColumnDescriptor[] families = descriptor.getColumnFamilies();

            for (HColumnDescriptor family : families) {
                String familyNameAsString = family.getNameAsString();
                System.out.println("\t" + familyNameAsString);
            }

        }

    }

    @Test
    public void put() throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put("zhc".getBytes());

        put.addColumn("info".getBytes(), "age".getBytes(), "18".getBytes());
        put.addColumn("info".getBytes(), "birthday".getBytes(), "1992-11-02".getBytes());
        put.addColumn("info".getBytes(), "company".getBytes(), "ALIBABA".getBytes());

        put.addColumn("address".getBytes(), "country".getBytes(), "CN".getBytes());
        put.addColumn("address".getBytes(), "province".getBytes(), "NMG".getBytes());
        put.addColumn("address".getBytes(), "city".getBytes(), "TL".getBytes());

        table.put(put);

    }


    @Test
    public void testGet01() throws Exception {

        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get("zhc".getBytes());
        Result result = table.get(get);

//        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("info".getBytes());
//
//        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
//            String k = Bytes.toString(entry.getKey());
//            String v = Bytes.toString(entry.getValue());
//
//            System.out.println(k + " : " + v);
//        }

        printResult(result);

        System.out.println("--------------------------");
    }


    @Test
    public void testScan01() throws Exception {

        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            printResult(result);

            System.out.println("--------------------------");
        }
    }


    public void printResult(Result result) {
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println(Bytes.toString(result.getRow()) + "\t"
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneValue(cell)) + "\t"
                    + cell.getTimestamp());
        }

    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }

}
