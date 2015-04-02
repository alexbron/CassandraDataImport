package com.perion.analytics.cassandra;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Created by ubuntu on 10/20/14.
 */
public class SSTableCreator {

    public static final int LINES_IN_BATCH = 1000000;
    final int hourly_ttl = 3 * 60 * 60 * 24;
    final int daily_ttl = 3 * 365* 60 * 60 * 24;


    public File createSSTableFiles(String csvFile, String keyspace, String columnFamily, MutableInt lineCounter, MutableBoolean lastLineRead, BufferedReader reader) throws IOException {

        File directory = new File(keyspace+columnFamily);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
        directory.mkdir();

        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new Murmur3Partitioner();

        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
        types.add(AsciiType.instance);
        types.add(AsciiType.instance);
        CompositeType cmpType = CompositeType.getInstance(types);



        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory, partitioner, keyspace, columnFamily, cmpType, null, 64);


        String line;
        CsvEntry entry = new CsvEntry(csvFile);
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;

        int currentCounter = 0;
        int ttl = daily_ttl;
        if(columnFamily.contains("hour"))
            ttl = hourly_ttl;


        while ((line = reader.readLine()) != null && currentCounter < LINES_IN_BATCH) {
            currentCounter++;
            if (entry.parse(line, lineCounter.intValue()+currentCounter)) {
                usersWriter.newRow(bytes(entry.partner_filter_gb1_gb2));
                usersWriter.addExpiringColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("dim_gb1")).build(), ByteBufferUtil.bytes(entry.dim_gb1), timestamp, ttl, (timestamp/1000) + ttl * 1000);
                usersWriter.addExpiringColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("dim_gb2")).build(), ByteBufferUtil.bytes(entry.dim_gb2), timestamp, ttl, (timestamp/1000) + ttl * 1000);
                usersWriter.addExpiringColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("f")).build(), ByteBufferUtil.bytes(entry.f), timestamp, ttl, (timestamp/1000) + ttl * 1000);
                for(int i=1; i < CsvEntry.max_allowed_measures;i++)
                {
                    usersWriter.addExpiringColumn(cmpType.builder().add(ByteBufferUtil.bytes(entry.dt_gb1_gb2)).add(ByteBufferUtil.bytes("m" + i)).build(), ByteBufferUtil.bytes(entry.getMeasures()[i]), timestamp, ttl, (timestamp/1000) + ttl * 1000);
                }
            }

        }
        lineCounter.add(currentCounter);
        if (reader.readLine()==null)    // last line
            lastLineRead.setValue(true);
        // Don't forget to close!
        usersWriter.close();
        return directory;
    }

    static class CsvEntry {
        public static int max_allowed_measures = 20;
        public Integer[] measures = new Integer[max_allowed_measures+1];
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

            try {
                partner_filter_gb1_gb2 = columns[0].trim();
                dt_gb1_gb2 = columns[1].trim();
                dim_gb1 = columns[2].trim();
                dim_gb2 = columns[3].trim();
                f = columns[4].trim();
                for(int i=1; i < max_allowed_measures;i++)
                {
                    measures[i] = Float.valueOf(columns.length <=4+i ? "0": columns[4+i].trim()).intValue();
                }


                return true;
            } catch (NumberFormatException e) {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, csvFile));
                return false;
            }
        }

        private Integer[] getMeasures() {
            return measures;
        }
    }
}
