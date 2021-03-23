package com.sy.base.abs;

import com.sy.util.SparkRead;
import com.sy.util.SparkWrite;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * @Author Shi Yan
 * @Date 2020/8/20 16:03
 */
public abstract class AbstractCsvDataProcess implements Serializable {
    public SparkSession session;
    public DataFrameReader reader;

    public AbstractCsvDataProcess(SparkSession session, DataFrameReader reader) {
        this.session = session;
        this. reader = reader;
    }

    /**
     * 读取csv数据
     * @param csvPath
     * @return
     */
    public Dataset<Row> csvDataRead(String csvPath) {
        Dataset<Row> dataset = SparkRead.readCsv(reader, csvPath);
        return dataset;
    }

    /**
     * 显示数据
     * @param dataset
     */
    public void showDataset(Dataset<Row> dataset) {
        dataset.show(10);
        dataset.printSchema();
    }

    /**
     * 统计某列去重后的个数
     * @param dataset
     * @param colName
     */
    public void countColDistinct(Dataset<Row> dataset, String colName) {
        dataset = dataset.groupBy(colName).count().orderBy(col("count").desc());
        dataset.show(20);
        System.out.println(dataset.count());
    }

    /**
     * 读取csv文件，并保存
     * @param fileToRead
     * @param fileToSave
     * @param header
     */
    public void readCsvData(String fileToRead, String fileToSave, String[] header) {
        readCsvData("", fileToRead, fileToSave, header);
    }

    /**
     * 读取csv文件，并保存
     * @param prefixName
     * @param fileToRead
     * @param fileToSave
     * @param header
     */
    public void readCsvData(String prefixName, String fileToRead, String fileToSave, String[] header) {
        Dataset<Row> dataset = csvDataRead(fileToRead);
        JavaRDD<String> javaRDD = dataset.toJavaRDD().mapPartitions(rowIterator -> {
            List<String> stringList = new ArrayList<>();
            StringBuffer sb;
            if(StringUtils.isNotBlank(prefixName)) {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    sb = new StringBuffer();
                    sb.append(prefixName + "_" + String.valueOf(row.get(0))).append(",");
                    for(int i = 1; i < row.size() - 1; i ++) {
                        sb.append(String.valueOf(row.get(i))).append(",");
                    }
                    sb.append(String.valueOf(row.get(row.size() - 1)));
                    stringList.add(sb.toString().trim());
                }
            } else {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    sb = new StringBuffer();
                    for(int i = 0; i < row.size() - 1; i ++) {
                        sb.append(String.valueOf(row.get(i))).append(",");
                    }
                    sb.append(String.valueOf(row.get(row.size() - 1)));
                    stringList.add(sb.toString().trim());
                }
            }
            return stringList.iterator();
        });
        SparkWrite.writeToOneFile(javaRDD, fileToSave, header, ",");
    }

}
