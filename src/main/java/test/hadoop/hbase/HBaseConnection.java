package test.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseConnection {

    private Connection connection;
    private Configuration configuration;
    private Admin admin;

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Connection getConnection() {
        if (connection == null) {
            configuration = HBaseConfiguration.create();
//            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "47.94.106.96");
            configuration.set("hbase.master", "47.94.106.96:2181");
            configuration.set("hbase.rpc.timeout", "1000");
            configuration.set("hbase.client.retries.number", "0");
//            File workaround = new File(".");
//            System.getProperties().put("hadoop.home.dir",
//                    workaround.getAbsolutePath());
//            new File("./bin").mkdirs();
//            try {
//                new File("./bin/winutils.exe").createNewFile();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            try {
                connection = ConnectionFactory.createConnection(configuration);
                admin = connection.getAdmin();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
