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
    private final String remoteFolder;
    private Session session;

//    public static void main(String[] args)  {
//        try {
//            ScpTo scpTo = new ScpTo(new File ("/home/ubuntu/projects/Analytics/lightspeed"), remoteFolder);
//            scpTo.copy();
//
//        } catch (JSchException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (SftpException e) {
//            e.printStackTrace();
//        }
//
//    }
    public ScpTo(File localFolder, String remoteFolder, String host, String username, String prvkey) throws JSchException, FileNotFoundException, SftpException {
        JSch jsch = new JSch();
        this.localFolder = localFolder;
        this.remoteFolder = remoteFolder;
        session = null;
        jsch.addIdentity(prvkey);

        session = jsch.getSession(username, host,22);
        session.setConfig("StrictHostKeyChecking", "no");
    }

    public  void copy() throws JSchException, SftpException, FileNotFoundException {
        session.connect();
        ChannelSftp channel = null;
        channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        channel.cd(remoteFolder);

        for (final File fileEntry : localFolder.listFiles()) {
            channel.put(new FileInputStream(fileEntry),fileEntry.getName());
        }
        channel.disconnect();
        session.disconnect();
    }

}
