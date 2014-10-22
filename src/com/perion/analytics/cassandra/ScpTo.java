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
    private ChannelSftp channel=null;

    //    public static void main(String[] args)  {
//        try {
//            ScpTo scpTo = new ScpTo(new File ("/home/ubuntu/projects/Analytics/lightspeed"), remoteFolder);
//            scpTo.move();
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

    public  void move() throws JSchException, SftpException, FileNotFoundException {
        initChannel();

        for (final File fileEntry : localFolder.listFiles()) {
            channel.put(new FileInputStream(fileEntry), fileEntry.getName());
            fileEntry.delete();

        }
    }

    public void rmRemoteFiles() throws SftpException, JSchException {
        initChannel();
        channel.rm("*");
    }


    public void disconnect() {
        channel.disconnect();
        session.disconnect();
    }

    public void initChannel() throws JSchException, SftpException {
        if(channel!=null)
            return ;
        session.connect();
        channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        channel.cd(remoteFolder);
    }

}
