package test.hadoop.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HbaseTable extends HBaseConnection {

    protected void createTable() throws IOException {
        Admin admin = getConnection().getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("emp3"));

        List<ColumnFamilyDescriptor> list = new LinkedList();
        list.add(ColumnFamilyDescriptorBuilder.of("text1"));
        tableDescriptorBuilder.setColumnFamilies(list);

        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("end");

    }

    protected TableName[] listTables() throws IOException {
        Admin admin = getConnection().getAdmin();
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }
        return tableNames;
    }

    protected void deleteAllTables() throws IOException {
        Admin admin = getConnection().getAdmin();
        for (TableName tableName : listTables()) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

    }

    public static void main(String[] args) throws IOException {
        HbaseTable table = new HbaseTable();
        table.deleteAllTables();
        table.createTable();
    }
}
