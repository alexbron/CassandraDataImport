package com.perion.analytics.cassandra;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.IOException;

/**
 * Created by ubuntu on 11/16/14.
 */
public interface CopyTo {
    void move() throws JSchException, SftpException, IOException;

    void rmRemoteFiles() throws SftpException, JSchException, IOException;

    void disconnect();
}
