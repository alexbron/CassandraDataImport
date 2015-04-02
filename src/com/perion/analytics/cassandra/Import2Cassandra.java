package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class Import2Cassandra {

    public void initLog()
    {
        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.FATAL);
        console.activateOptions();
        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(console);

        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile("/tmp/mylog.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.ERROR);
        fa.setAppend(true);
        fa.activateOptions();

        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(fa) ;
        //repeat with all other desired appenders
    }
    public static void main(String[] args) throws Exception {
        final String keyspace = "lightspeed";
        final String columnFamily = "analytics_stg_active_users_hourly";
        final String csvFile = "/cassandra_tmp/s3_file_active_users_hourly_10.csv";
        final String remoteFolderRoot = "/tmp/Alex/";
        final String remoteHost = "172.26.3.84";
        final int jmxPort = 7199;
        final String username = "ec2-user";
        final String prvkey = "/home/ubuntu/.ssh/newplatform-us.pem";


        new Import2Cassandra().importData(keyspace, columnFamily, csvFile, remoteHost, jmxPort, username, prvkey, remoteFolderRoot, 1);
    }

    public void importData(String keyspace, String columnFamily, String csvFile, String remoteHost, int jmxPort, String username, String prvkey, String remoteFolderRoot, Integer remoteCopy) throws Exception {
        initLog();
        System.out.print(keyspace + " " + columnFamily+" " + csvFile+" " +  remoteHost+" " +  jmxPort+" " +  username+" " +  prvkey+" " +  remoteFolderRoot+" " +  remoteCopy);
        final String remoteFolder = remoteFolderRoot + "/" + keyspace + "/" + columnFamily;
        final SSTableCreator ssTableCreator = new SSTableCreator();
        CopyTo copyTo = null;
        JmxBulkLoader np = null;
        Logger log = Logger.getLogger(columnFamily);
        log.error("Start import");
        try {
            MutableInt lineCounter = new MutableInt(0);
            MutableBoolean lastLineRead = new MutableBoolean(false);
            final BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            while(!lastLineRead.booleanValue()) {

                File sstableFolder = ssTableCreator.createSSTableFiles(csvFile, keyspace, columnFamily, lineCounter, lastLineRead, reader);
                if (copyTo==null)
                    copyTo = createCopyObject(remoteHost, username, prvkey, remoteCopy != 0, remoteFolder, sstableFolder);
                try {
                    log.error("Start Copying, line counter " + lineCounter.intValue());
                    copyTo.move();
                    log.error("End Copying");
                    if (np==null)
                        np = new JmxBulkLoader(remoteHost, jmxPort);
                    log.error("Bulk Loading start");
                    np.bulkLoad(remoteFolder);
                    log.error("Bulk Loading end");
                } finally {
                    copyTo.rmRemoteFiles();
                }
            }
            log.error("End import");
        } catch (Exception e) {
            log.error("An exception was thrown in Import2Cassandra::importData:",e);
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