package com.sy.util;

import com.sy.init.InitMysql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @Author Shi Yan
 * @Date 2020/8/13 11:51
 */
public class SparkWrite implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SparkWrite.class);

    /**
     * 保存csv数据
     * @param dataset
     * @param pathToWrite
     */
    public static void writeToCsvFile(Dataset<Row> dataset, String pathToWrite) {
        dataset.coalesce(1).write().mode(SaveMode.Append)
                .format("CSV")
                .option("header", "true")
                .option("delimiter", ",")
                .save(pathToWrite);
        System.out.println("dataset be saved to " + pathToWrite);
    }

    /**
     * 保存到mysql中
     * @param dataset
     * @param table
     */
    public static void writeToMysql(Dataset<Row> dataset, String table) {
        dataset.repartition(1).write().mode(SaveMode.Append)
                .jdbc(InitMysql.mysql_hostname, table, InitMysql.connectionProperties);
    }

    /**
     * 保存json数据
     * @param dataset
     * @param pathToWrite
     */
    public static void writeToJsonFile(Dataset<Row> dataset, String pathToWrite) {

    }

    /**
     * 保存parquet数据
     * @param dataset
     * @param pathToWrite
     */
    public static void writeToParquet(Dataset<Row> dataset, String pathToWrite) {

    }

    public static void writeToOneFile(JavaRDD<String> javaRDD, String pathToWrite, String[] header) {
        writeToOneFile(javaRDD, pathToWrite, header, ",");
    }

    /**
     * 保存数据
     * @param javaRDD
     * @param pathToWrite
     * @param header
     * @param delimiter
     */
    public static void writeToOneFile(JavaRDD<String> javaRDD, String pathToWrite, String[] header, String delimiter) {
        int partitionNum = javaRDD.getNumPartitions();
        Print.print("partitions: " + partitionNum);
        Path path = Paths.get(pathToWrite);
        try {
            Files.deleteIfExists(path);
            Files.createFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try(BufferedWriter bw = Files.newBufferedWriter(path)) {
            if(0 != header.length) {
                StringBuffer sb = new StringBuffer();
                for(int i = 0; i < header.length - 1; i++) {
                    sb.append(header[i]).append(delimiter);
                }
                sb.append(header[header.length - 1]);
                bw.write(sb.toString().trim());
                bw.newLine();
            }
            for(int i =0; i < partitionNum; i++) {
                int[] index = new int[1];
                index[0] = i;
                List<String>[] line = javaRDD.collectPartitions(index);
                if(1 != line.length) {
                    logger.error("[writeToString] >> line is not 1!");
                }
                for(String str : line[0]) {
                    bw.write(str);
                    bw.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
