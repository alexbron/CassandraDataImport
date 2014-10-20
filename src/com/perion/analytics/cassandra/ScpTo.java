package com.perion.analytics.cassandra;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


/**
 * Created by ubuntu on 10/15/14.
 */
public class ScpTo {

    private final File localFolder;
    private Session session;

    public static void main(String[] args)  {
        try {
            ScpTo scpTo = new ScpTo(new File ("/home/ubuntu/projects/Analytics/lightspeed"));
            scpTo.copy();

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }

    }
    public ScpTo(File localFolder) throws JSchException, FileNotFoundException, SftpException {
        JSch jsch = new JSch();
        this.localFolder = localFolder;
        session = null;
//        session = jsch.getSession("root","172.16.3.121",22);
//        session.setPassword("Q1w2e3r$");
        jsch.addIdentity("/home/ubuntu/projects/CassandraDataImport2/touchbean-virginia.pem");

        session = jsch.getSession("ec2-user","172.19.2.251",22);
        session.setConfig("StrictHostKeyChecking", "no");
    }

    public  void copy() throws JSchException, SftpException, FileNotFoundException {
        session.connect();
        ChannelSftp channel = null;
        channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        channel.cd("/home/ec2-user/Alex/lightspeed/analytics_dev_active_users_hourly");

        for (final File fileEntry : localFolder.listFiles()) {
            channel.put(new FileInputStream(fileEntry),fileEntry.getName());
        }
        channel.disconnect();
        session.disconnect();
    }

}
