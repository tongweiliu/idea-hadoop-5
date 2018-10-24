package com.it18zhang.hbasedemo.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class TestCRUD {


    @Test
    public void testColFilter()throws Exception{
        Configuration conf=HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t7");

        Scan scan = new Scan();
        QualifierFilter colfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("id")));
        scan.setFilter(colfilter);
        Table t=conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + f2id + " : " + f2name);

        }
    }

    @Test
    public void testFamilyFilter()throws Exception{
       Configuration conf=HBaseConfiguration.create();
       Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t7");
        Scan scan = new Scan();
        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("f2")));
        scan.setFilter(filter);
        Table t=conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r =  it.next();
            byte[] flid = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            System.out.println(flid+":"+f2id);

        }
    }
    @Test
    public  void testRowFilter()throws Exception{
        Configuration conf=HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname=TableName.valueOf("ns5:t5");
        Table table=conn.getTable(tname);

        Scan scan=new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row0100")));
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            System.out.println(Bytes.toString(result.getRow()));
        }
    }
    @Test
    public void scan3()throws Exception{
        Configuration conf=HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname=TableName.valueOf("ns5:t5");
        Table table=conn.getTable(tname);
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes("row5000"));
        scan.setStopRow(Bytes.toBytes("row8000"));
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            //得到一行的所有map，key=f1,value=Map<Col,Map<Timestamp,value>>
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();

            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                //得到列族
                String f=Bytes.toString(entry.getKey());
                Map<byte[], NavigableMap<Long, byte[]>> colDataMap = entry.getValue();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> ets : colDataMap.entrySet()) {

                    String c=Bytes.toString(ets.getKey());
                    Map<Long, byte[]> tsValueMap = ets.getValue();
                    for (Map.Entry<Long, byte[]> e : tsValueMap.entrySet()) {
                        Long ts=e.getKey();
                        String value=Bytes.toString(e.getValue());
                        System.out.print(f + ":" + c + ":" + ts + "=" + value + ",");
                    }
                }
            }
            System.out.println();
        }
    }

    @Test
    public void scan2()throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname=TableName.valueOf("ns5:t5");
        Table table=conn.getTable(tname);

        Scan scan =new Scan();
        scan.setStartRow(Bytes.toBytes("row5000"));
        scan.setStopRow(Bytes.toBytes("row8000"));

        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            Map<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("f1"));
            for (Map.Entry<byte[], byte[]> entrySet : map.entrySet()) {
                String col=Bytes.toString(entrySet.getKey());
                String val=Bytes.toString(entrySet.getValue());
                System.out.print(col+":"+val+",");
            }
            System.out.println();
        }
    }
    @Test
    public void  scan()throws Exception{
        Configuration conf=HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tname=TableName.valueOf("ns5:t5");
        Table table=conn.getTable(tname);
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes("row5000"));
        scan.setStopRow(Bytes.toBytes("row8000"));
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[]name=r.getValue(Bytes.toBytes("f1"),Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));

        }
    }
    @Test
    public void listNameSpaces() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n : ns) {
            System.out.println(n.getName());
        }
    }

    @Test
    public void createNameSpace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns5").build();
        admin.createNamespace(nsd);

        NamespaceDescriptor[] nd = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n : nd) {
            System.out.println(n.getName());
        }
    }

    @Test
    public void get() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");

        Table table = conn.getTable(tname);

        byte[] rowid = Bytes.toBytes("row3");
        Get get = new Get(rowid);
        Result r = table.get(get);
        byte[] idValue = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
        System.out.println(Bytes.toInt(idValue));
    }

    @Test
    public void put() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");

        Table table = conn.getTable(tname);

        byte[] rowid = Bytes.toBytes("row3");
        Put put = new Put(rowid);
        byte[] f1 = Bytes.toBytes("f1");
        byte[] id = Bytes.toBytes("id");
        byte[] value = Bytes.toBytes(102);

        put.addColumn(f1, id, value);
        table.put(put);
    }
}
