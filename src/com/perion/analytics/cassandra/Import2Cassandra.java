package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

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
        fa.setThreshold(Level.DEBUG);
        fa.setAppend(true);
        fa.activateOptions();

        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(fa) ;
        //repeat with all other desired appenders
    }

   public static void main (String[] args) throws IOException, SftpException, JSchException {
       File sstableFolder = new Import2Cassandra().createSSTableFiles("lightspeed","/tmp/s3_file_active_users_hourly.csv");
       ScpTo scpTo = new ScpTo(sstableFolder);
       scpTo.copy();
    }



    public void foo (String keyspace, String csvFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        File directory = new File(keyspace);
        if (!directory.exists()) {
            directory.mkdir();
        }

        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new Murmur3Partitioner();

        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory, partitioner, keyspace, "analytics_dev_active_users_hourly", AsciiType.instance, null, 64);
    }
    public File createSSTableFiles(String keyspace, String csvFile) throws IOException {
        System.out.print("hello");
        initLog();
        System.setProperty("cassandra.config","file:///home/ubuntu/projects/CassandraDataImport2/cassandra.yaml");
        System.setProperty("dse.config","file:///home/ubuntu/projects/CassandraDataImport2/dse.yaml");
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        File directory = new File(keyspace);
        if (!directory.exists()) {
            directory.mkdir();
        }

        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new Murmur3Partitioner();
/*
        new
         */
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
        types.add(AsciiType.instance);
        types.add(AsciiType.instance);
        CompositeType cmpType = CompositeType.getInstance(types);
        /*
        new

         */


        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory, partitioner, keyspace, "analytics_dev_active_users_hourly", cmpType, null, 64);
//
//        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
//                directory, partitioner, keyspace, "analytics_dev_active_users_hourly", AsciiType.instance, null, 64);

        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry(csvFile);
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;

        while ((line = reader.readLine()) != null) {
            if (entry.parse(line, lineNumber)) {

//                ByteBuffer uuid = ByteBuffer.wrap(decompose(UUID.fromString(entry.partner_filter_gb1_gb2.replace('-',':'))));
                usersWriter.newRow(bytes(entry.partner_filter_gb1_gb2));
//                usersWriter.addColumn(ByteBufferUtil.bytes("partner_filter_gb1_gb2"), ByteBufferUtil.bytes(entry.partner_filter_gb1_gb2), timestamp);
//                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes("dt_gb1_gb2")).add(ByteBufferUtil.bytes("dt_gb1_gb2")).build(), ByteBufferUtil.bytes(entry.dt_gb1_gb2), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("dim_gb1")).build(), ByteBufferUtil.bytes(entry.dim_gb1), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("dim_gb2")).build(),ByteBufferUtil.bytes(entry.dim_gb2), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("f")).build(), ByteBufferUtil.bytes(entry.f), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m1")).build(), ByteBufferUtil.bytes(entry.m1), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m2")).build(),ByteBufferUtil.bytes(entry.m2), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m3")).build(), ByteBufferUtil.bytes(entry.m3), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m4")).build(), ByteBufferUtil.bytes(entry.m4), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m5")).build(), ByteBufferUtil.bytes(entry.m5), timestamp);
                usersWriter.addColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m6")).build(), ByteBufferUtil.bytes(entry.m6), timestamp);
            }
            lineNumber++;
        }
        // Don't forget to close!
        usersWriter.close();
        return directory;
    }

    static class CsvEntry {
        public String m1, m2, m3, m4, m5, m6, m7, m8, m9, m10;
        public String partner_filter_gb1_gb2;
        public String dt_gb1_gb2;
        public String dim_gb1;
        public String dim_gb2;
        public String f;
        private String csvFile;

        public CsvEntry(String csvFile) {
                this.csvFile = csvFile;
        }

        boolean parse(String line, int lineNumber) {
            // Ghetto csv parsing
            String[] columns = line.split(",");
//            if (columns.length != 6)
//            {
//                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
//                return false;
//            }
            try {
                partner_filter_gb1_gb2 = columns[0].trim();
                dt_gb1_gb2 = columns[1].trim();
                dim_gb1 = columns[2].trim();
                dim_gb2 = columns[3].trim();
                f = columns[4].trim();
                m1 = columns[5].trim();
                m2 = columns[6].trim();
                m3 = columns[7].trim();
                m4 = columns[8].trim();
                m5 = columns[9].trim();
                m6 = columns[10].trim();
//                m7 = columns[11].trim();
//                m8 = columns[12].trim();
//                m9 = columns[13].trim();
//                m10 = columns[14].trim();

                return true;
            } catch (NumberFormatException e) {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, csvFile));
                return false;
            }
        }
    }
}