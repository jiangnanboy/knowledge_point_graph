package com.sy.dataprocess;

import com.sy.base.abs.AbstractCsvDataProcess;
import com.sy.init.InitSchema;
import com.sy.util.SparkRead;
import com.sy.util.SparkWrite;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 处理csv数据
 * @Author Shi Yan
 * @Date 2020/8/20 15:56
 */
public class CsvDataProcess extends AbstractCsvDataProcess {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvDataProcess.class);

    public CsvDataProcess(SparkSession session) {
        this(session, SparkRead.csvReader(session));
    }

    public CsvDataProcess(SparkSession session, String delimiter) {
        this(session, SparkRead.csvReader(session, delimiter));
    }

    public CsvDataProcess(SparkSession session, DataFrameReader reader) {
        super(session, reader);
    }

    public void processCsvData(String fileToRead, String fileToWrite) {
        processCsvData(fileToRead, fileToWrite, new String[]{});
    }

    /**
     * 处理csv数据
     * @param fileToRead
     * @param fileToWrite
     * @param header
     */
    public void processCsvData(String fileToRead, String fileToWrite, String[] header) {
        Dataset<Row> dataset = csvDataRead(fileToRead);
    }

    /**
     * 处理book数据
     * @param fileToRead
     * @param  fileToWrite
     */
    public void bookProcess(String fileToRead, String fileToWrite) {
        Dataset<Row> dataset = csvDataRead(fileToRead);
        dataset.persist(StorageLevel.MEMORY_AND_DISK());
        /**
         * 1.对book进行处理，将数据存储到map中，<key,value> : key -> id+stage+course_id, value -> name
         * 2.将相同name的教材处理成<key,value> : key -> id, key -> name
         * 3.将1和2对应起来
         */
        //<key,value> : key -> id+stage+course_id, value -> name
        ConcurrentMap<String,String> bookRawMap = new ConcurrentHashMap<>(31);
        //<key,value> : key -> id, key -> name
        ConcurrentMap<String, String> idNameBookMap = new ConcurrentHashMap<>(31);
        //<key,value> : key -> id+stage+course_id, value -> id
        ConcurrentMap<String, String> bookRawIDMap = new ConcurrentHashMap<>(31);



        Broadcast<ConcurrentMap<String, String>> broadcastBookRawMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(bookRawMap);
        Broadcast<ConcurrentMap<String, String>> broadcastIdNameBookMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(idNameBookMap);
        Broadcast<ConcurrentMap<String, String>> broadcastBookRawIDMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(bookRawIDMap);


    }

    /**
     * tag与id映射
     * @param dataset
     * @param tagIdPath
     */
    public void tagToId(Dataset<Row> dataset, String tagIdPath) {
        JavaRDD<Row> rowRdd = dataset.toJavaRDD().map((Function<Row, Row>) row -> {
            return RowFactory.create("tag_"+String.valueOf(row.getInt(0)), row.getAs(1));
        });
        Dataset<Row> tagRow = session.createDataFrame(rowRdd, InitSchema.initSchema());
        showDataset(tagRow);
        SparkWrite.writeToCsvFile(tagRow, tagIdPath);
    }

}
