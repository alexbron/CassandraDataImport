package com.perion.analytics.cassandra;

import java.io.File;

public class Import2Cassandra {





   public static void main (String[] args) throws Exception {
       final String keyspace = "lightspeed";
       final String columnFamily = "analytics_dev_active_users_hourly";
       final String csvFile = "/tmp/s3_file_active_users_hourly.csv";
       final String remoteFolderRoot = "/tmp/Alex/";
       final String remoteHost = "172.19.2.251";
       final int jmxPort = 7199;
       final String username = "ec2-user";
       final String prvkey = "/home/ubuntu/projects/CassandraDataImport2/touchbean-virginia.pem";

       new Import2Cassandra().importData(keyspace, columnFamily, csvFile, remoteHost, jmxPort, username, prvkey, remoteFolderRoot);
    }

    public  void importData(String keyspace, String columnFamily, String csvFile, String remoteHost, int jmxPort, String username, String prvkey, String remoteFolderRoot) throws Exception {
        final String remoteFolder = remoteFolderRoot + "/" + keyspace + "/" + columnFamily;
        final SSTableCreator ssTableCreator = new SSTableCreator();
        File sstableFolder = ssTableCreator.createSSTableFiles(csvFile, keyspace, columnFamily);

        ScpTo scpTo = new ScpTo(sstableFolder,remoteFolder, remoteHost, username, prvkey);
        scpTo.copy();

        JmxBulkLoader np = new JmxBulkLoader(remoteHost, jmxPort);
        np.bulkLoad(remoteFolder);
    }


}