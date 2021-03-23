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
 * @Date 2020/8/18 9:32
 */
public abstract class AbstractMysqlDataProcess implements Serializable {

    public SparkSession session;
    public DataFrameReader reader;

    public AbstractMysqlDataProcess(SparkSession session, DataFrameReader reader) {
        this.session = session;
        this.reader = reader;
    }

    /**
     * 读取mysql数据
     * @param sql
     * @return
     */
    public Dataset<Row> mysqlDataRead(String sql) {
        Dataset<Row> dataset = SparkRead.readMysql(reader, sql);
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
        dataset.groupBy(colName).count().orderBy(col("count").desc()).show();
        System.out.println(dataset.distinct().count());
    }

    /**
     * 读取mysql表，并保存
     * @param sql
     * @param fileToSave
     * @param header
     */
    public void readTableData(String sql, String fileToSave, String[] header) {
        readTableData("", sql, fileToSave, header);
    }

    /**
     * 读取mysql表，并保存
     * @param prifixName 为id前缀,如prifixName_id
     * @param sql
     * @param fileToSave
     * @param header
     */
    public void readTableData(String prifixName, String sql, String fileToSave, String[] header) {
        Dataset<Row> dataset = mysqlDataRead(sql);
        JavaRDD<String> javaRDD = dataset.toJavaRDD().mapPartitions(rowIterator -> {
            List<String> stringList = new ArrayList<>();
            StringBuffer sb;
            if(StringUtils.isNotBlank(prifixName)) {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    sb = new StringBuffer();
                    sb.append(prifixName + "_" + String.valueOf(row.get(0))).append(",");
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
