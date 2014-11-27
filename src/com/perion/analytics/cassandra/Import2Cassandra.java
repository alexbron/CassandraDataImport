package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.File;
import java.io.FileNotFoundException;
//import java.nio.file.Files;
//import java.nio.file.StandardCopyOption;

public class Import2Cassandra {


    public static void main(String[] args) throws Exception {
        final String keyspace = "lightspeed";
        final String columnFamily = "analytics_dev_usage_daily";
        final String csvFile = "/tmp/s3_file_usage_daily.csv";
        final String remoteFolderRoot = "/tmp/Alex/";
        final String remoteHost = "172.26.31.249";
        final int jmxPort = 7199;
        final String username = "ec2-user";
        final String prvkey = "/home/ubuntu/.ssh/newplatform-us.pem";


        new Import2Cassandra().importData(keyspace, columnFamily, csvFile, remoteHost, jmxPort, username, prvkey, remoteFolderRoot, 1);
    }

    public void importData(String keyspace, String columnFamily, String csvFile, String remoteHost, int jmxPort, String username, String prvkey, String remoteFolderRoot, Integer remoteCopy) throws Exception {
        System.out.print(keyspace + " " + columnFamily+" " + csvFile+" " +  remoteHost+" " +  jmxPort+" " +  username+" " +  prvkey+" " +  remoteFolderRoot+" " +  remoteCopy);
        final String remoteFolder = remoteFolderRoot + "/" + keyspace + "/" + columnFamily;
        final SSTableCreator ssTableCreator = new SSTableCreator();
        CopyTo copyTo = null;
        try {
            File sstableFolder = ssTableCreator.createSSTableFiles(csvFile, keyspace, columnFamily);
            copyTo = createCopyObject(remoteHost, username, prvkey, remoteCopy != 0 , remoteFolder, sstableFolder);
            copyTo.move();
            JmxBulkLoader np = new JmxBulkLoader(remoteHost, jmxPort);
            np.bulkLoad(remoteFolder);
            copyTo.rmRemoteFiles();

        } catch (Exception e) {
            System.out.print(e);
        } finally {
            if (copyTo != null)
                copyTo.disconnect();
        }
    }

    public CopyTo createCopyObject(String remoteHost, String username, String prvkey, boolean remoteCopy, String remoteFolder, File sstableFolder) throws JSchException, FileNotFoundException, SftpException {
        CopyTo copyTo;
        if (remoteCopy) {
            copyTo = new ScpTo(sstableFolder, remoteFolder, remoteHost, username, prvkey);
        } else {
            copyTo = new LocalCpTo(sstableFolder,remoteFolder);
        }
        return copyTo;
    }


}