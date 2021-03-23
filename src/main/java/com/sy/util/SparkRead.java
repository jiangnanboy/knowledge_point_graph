package com.sy.util;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sy.init.InitMysql;

import java.io.Serializable;

/**
 * Read data from mysql,csv,json and so on.
 * @Author Shi Yan
 * @Date 2020/8/13 10:14
 */
public class SparkRead implements Serializable {

    /**
     * 初始化 mysql reader
     * @param session
     * @param host
     * @param port
     * @param database
     * @param user
     * @param pass
     * @return
     */
    public static DataFrameReader mysqlReader(SparkSession session, String host, String port, String database, String user, String pass) {
        DataFrameReader reader = session.read().format("jdbc")
                .option("url", "jdbc:mysql://" + host + ":" + port + "/" + database+"?useUnicode=true&characterEncoding=utf-8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", user)
                .option("password", pass);
        return reader;
    }


    /**
     * 初始化 mysql reader
     * @param session
     * @return
     */
    public static DataFrameReader mysqlReader(SparkSession session) {
        DataFrameReader reader = session.read().format("jdbc")
                .option("url", "jdbc:mysql://" + InitMysql.mysql_hostname + ":" + InitMysql.mysql_port + "/" + InitMysql.mysql_database+"?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", InitMysql.mysql_username)
                .option("password", InitMysql.mysql_password);
        return reader;
    }

    /**
     * read Mysql
     * note:1.避免读取不必要的数据，造成mysql卡死或spark oom;2.读取大表时避免数据倾斜，可将数据分区处理。
     * @param reader
     * @param table
     * @return
     */
    public static Dataset<Row> readMysql(DataFrameReader reader, String table) {
        Dataset<Row> dataset = null;
        dataset = reader.option("dbtable", table).load();
        return dataset;
    }

    /**
     * csv格式默认分割符为","
     * @param session
     * @return
     */
    public static DataFrameReader csvReader(SparkSession session) {
        return csvReader(session, ",");
    }

    /**
     * 初始化 csv reader
     * @param session
     * @return
     */
    public static DataFrameReader csvReader(SparkSession session, String delimiter) {
        DataFrameReader reader = session.read().format("csv")
                .option("sep", delimiter)
                .option("inferSchema", true)
                .option("header", "true");
        return reader;
    }

    /**
     * read csv
     * @param reader
     * @param csvPath
     * @return
     */
    public static Dataset<Row> readCsv(DataFrameReader reader, String csvPath) {
        Dataset<Row> dataset = reader.load(csvPath);
        return dataset;
    }

    /**
     * 初始化 json reader
     * @param session
     * @return
     */
    public static DataFrameReader jsonReader(SparkSession session) {
        DataFrameReader reader = session.read().format("json");
        return reader;
    }

    /**
     * read json
     * @param reader
     * @param jsonPath
     * @return
     */
    public static Dataset<Row> readJson(DataFrameReader reader, String jsonPath) {
        Dataset<Row> dataset = reader.load(jsonPath);
        return dataset;
    }

    /**
     * 初始化 parquet reader
     * @param session
     * @return
     */
    public static DataFrameReader parquetReader(SparkSession session) {
        DataFrameReader reader = session.read().format("parquet");
         return reader;
    }

    /**
     * read parquet
     * @param reader
     * @param parquetPath
     * @return
     */
    public static Dataset<Row> readParquet(DataFrameReader reader, String parquetPath) {
        Dataset<Row> dataset = reader.load(parquetPath);
        return dataset;
    }

}
