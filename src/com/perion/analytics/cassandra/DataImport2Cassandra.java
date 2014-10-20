package com.perion.analytics.cassandra;


import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class DataImport2Cassandra {
    static String filename;

    public static void main(String[] args) throws IOException {

        new DataImport2Cassandra().createSSTableFiles("lightspeed","/tmp/s3_file_active_users_hourly.csv");

        System.exit(0);
    }

    public void importCsv (String keyspace, String csvFile) throws IOException {
          File directory = createSSTableFiles(keyspace,csvFile);
          copyToCassandraServer(directory);
    }

    private void copyToCassandraServer(File directory) {

    }

    public File createSSTableFiles(String keyspace, String csvFile) throws IOException {
//        System.setProperty("cassandra.config","///home/ubuntu/projects/CassandraDataImport2/cassandra1.yaml");
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        File directory = new File(keyspace);
        if (!directory.exists()) {
            directory.mkdir();
        }

        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new RandomPartitioner();

        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory, partitioner, keyspace, "analytics_dev_active_users_daily", AsciiType.instance, null, 64);

        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;

        while ((line = reader.readLine()) != null) {
            if (entry.parse(line, lineNumber)) {
//                ByteBuffer uuid = ByteBuffer.wrap(decompose(UUID.fromString(entry.partner_filter_gb1_gb2.replace('-',':'))));
                usersWriter.newRow(bytes(entry.partner_filter_gb1_gb2));
                usersWriter.addColumn(bytes("partner_filter_gb1_gb2"), bytes(entry.partner_filter_gb1_gb2), timestamp);
                usersWriter.addColumn(bytes("dt_gb1_gb2"), bytes(entry.dt_gb1_gb2), timestamp);
                usersWriter.addColumn(bytes("dim_gb1"), bytes(entry.dim_gb1), timestamp);
                usersWriter.addColumn(bytes("dim_gb2"), bytes(entry.dim_gb2), timestamp);
                usersWriter.addColumn(bytes("f"), bytes(entry.f), timestamp);
                usersWriter.addColumn(bytes("m1"), bytes(entry.m1), timestamp);
                usersWriter.addColumn(bytes("m2"), bytes(entry.m1), timestamp);
                usersWriter.addColumn(bytes("m3"), bytes(entry.m1), timestamp);
                usersWriter.addColumn(bytes("m4"), bytes(entry.m1), timestamp);
                usersWriter.addColumn(bytes("m5"), bytes(entry.m1), timestamp);
                usersWriter.addColumn(bytes("m6"), bytes(entry.m1), timestamp);
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
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
        }
    }
}