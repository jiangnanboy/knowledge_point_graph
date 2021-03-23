package com.sy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * @Author Shi Yan
 * @Date 2020/8/14 16:48
 */
public class HdfsOperate {
    private static Logger logger = LoggerFactory.getLogger(HdfsOperate.class);
    private static Configuration conf = new Configuration();
    private static BufferedWriter bw = null;

    public static void openHdfsFile(String path) throws Exception{
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        if(null != bw) {
            logger.info("HdfsOperate >> initialize writer succeed!");
        }
    }

    public static void writeLine(String line) {
        try {
            bw.write(line + "\n");
        } catch (Exception e) {
            logger.error("HdfsOperate >> writer a line error:", e);
        }
    }

    public static void closeHdfsFile() {
        try {
            if (null != bw) {
                bw.close();
                logger.info("[HdfsOperate]>> closeHdfsFile close writer succeed!");
            }
            else{
                logger.error("[HdfsOperate]>> closeHdfsFile writer is null");
            }
        }catch(Exception e){
            logger.error("[HdfsOperate]>> closeHdfsFile close hdfs error:" + e);
        }
    }
}

