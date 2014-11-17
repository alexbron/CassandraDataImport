package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.moveDirectory;


/**
 * Created by ubuntu on 10/15/14.
 */
public class LocalCpTo implements CopyTo {

    private final File localFolder;
    private final String remoteFolder;

    public LocalCpTo(File localFolder, String remoteFolder) throws JSchException, FileNotFoundException, SftpException {
        this.localFolder = localFolder;
        this.remoteFolder = remoteFolder;

    }

    @Override
    public  void move() throws JSchException, SftpException, IOException {
        rmRemoteFiles();
        moveDirectory(localFolder, new File(remoteFolder));
    }



    @Override
    public void rmRemoteFiles() throws SftpException, JSchException, IOException {
        FileUtils.deleteDirectory(new File(remoteFolder));
    }


    @Override
    public void disconnect() {
    }



}
