package com.perion.analytics.cassandra;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


/**
 * Created by ubuntu on 10/15/14.
 */
public class ScpTo implements CopyTo {

    private final File localFolder;
    private final String remoteFolder;
    private Session session;
    private ChannelSftp channel=null;

    public ScpTo(File localFolder, String remoteFolder, String host, String username, String prvkey) throws JSchException, FileNotFoundException, SftpException {
        JSch jsch = new JSch();
        this.localFolder = localFolder;
        this.remoteFolder = remoteFolder;
        session = null;
        jsch.addIdentity(prvkey);

        session = jsch.getSession(username, host,22);
        session.setConfig("StrictHostKeyChecking", "no");
    }

    @Override
    public  void move() throws JSchException, SftpException, FileNotFoundException {
        initChannel();
        mkTargetFolder();
        for (final File fileEntry : localFolder.listFiles()) {
            final String remoteFilePath = remoteFolder + "/" + fileEntry.getName();
            try {
                channel.put(new FileInputStream(fileEntry), remoteFilePath);
            }
            catch (SftpException e) {
                mkTargetFolder();
                channel.put(new FileInputStream(fileEntry), remoteFilePath);
            }
            finally {
                fileEntry.delete();
            }


        }
    }

    private void mkTargetFolder() throws SftpException {
        System.out.println("Creating Directory...");
        String[] complPath = remoteFolder.split("/");
        channel.cd("/");
        for (String dir : complPath) {
            if (dir.length() > 0) {
                try {
                    System.out.println("Current Dir : " + channel.pwd());
                    channel.cd(dir);
                } catch (SftpException e2) {
                    channel.mkdir(dir);
                    channel.cd(dir);
                }
            }
        }
        channel.rm(remoteFolder+"/*"); // clean target folder , if files were there before
        channel.cd("/");
        System.out.println("Current Dir : " + channel.pwd());
    }

    @Override
    public void rmRemoteFiles() throws SftpException, JSchException {
        initChannel();
        channel.rm(remoteFolder + "/" + "*");
    }


    @Override
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
//        channel.cd(remoteFolder);
    }

}
