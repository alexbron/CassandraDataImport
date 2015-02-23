package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class Import2Cassandra {


    public static void main(String[] args) throws Exception {
        final String keyspace = "lightspeed";
        final String columnFamily = "analytics_stg_funnel_daily";
        final String csvFile = "/tmp/s3_file_funnel_daily.csv";
        final String remoteFolderRoot = "/tmp/Alex/";
        final String remoteHost = "172.26.3.84";
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
        JmxBulkLoader np = null;
        try {
            MutableInt lineCounter = new MutableInt(0);
            MutableBoolean lastLineRead = new MutableBoolean(false);
            final BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            while(!lastLineRead.booleanValue()) {
                File sstableFolder = ssTableCreator.createSSTableFiles(csvFile, keyspace, columnFamily, lineCounter, lastLineRead, reader);
                if (copyTo==null)
                    copyTo = createCopyObject(remoteHost, username, prvkey, remoteCopy != 0, remoteFolder, sstableFolder);
                try {
                    copyTo.move();
                    if (np==null)
                        np = new JmxBulkLoader(remoteHost, jmxPort);

                    np.bulkLoad(remoteFolder);
                } finally {
                    copyTo.rmRemoteFiles();
                }
            }

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