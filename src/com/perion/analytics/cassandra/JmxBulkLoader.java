package com.perion.analytics.cassandra;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.log4j.*;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class JmxBulkLoader {
    private JMXConnector connector;
    private StorageServiceMBean storageBean;
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

    public JmxBulkLoader(String host, int port) throws Exception    {
        connect(host, port);
        initLog();
    }
    private void connect(String host, int port) throws IOException, MalformedObjectNameException    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port));
        Map<String,Object> env = new HashMap<String,Object>();
        connector = JMXConnectorFactory.connect(jmxUrl, env);
        MBeanServerConnection mbeanServerConn = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("org.apache.cassandra.db:type=StorageService");
        storageBean = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
    }

    public void close() throws IOException    {
        connector.close();
    }


    public void bulkLoad(String path) {
        storageBean.bulkLoad(path);
    }


    public static void main(String[] args) throws Exception {
//        if (args.length == 0) {
//            throw new IllegalArgumentException("usage: paths to bulk files");
//        }


        JmxBulkLoader np = new JmxBulkLoader("172.16.3.121", 7199);
        np.bulkLoad("/root/Alex/lightspeed/analytics_dev_active_users_daily/");
//        for (String arg : args) {
//            np.bulkLoad(arg);
//        }
        np.close();
    }
}