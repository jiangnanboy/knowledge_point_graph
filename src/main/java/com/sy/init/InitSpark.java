package com.sy.init;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Shi Yan
 * @Date 2020/8/6 13:52
 */
public class InitSpark {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitSpark.class);

    private static SparkSession sparkSession = null;
    private static JavaSparkContext javaSparkContext = null;
    private static SparkConf sparkConf = null;

    private static void initSparkConf() {
        LOGGER.info("Init SparkConf...");
        sparkConf = new SparkConf()
                .setAppName("ProcessData")
                .setMaster("local[*]")
                .set("spark.driver.memory", "4g")
                .set("spark.executor.memory", "4g")
                .set("spark.network.timeout", "1000")
                .set("spark.sql.broadcastTimeout", "2000")
                .set("spark.executor.heartbeatInterval", "1000")
                .set("spark.driver.maxResultSize", "4g")
                //rdd持久化操作使用压缩机制(只有在序列化后的rdd才能使用压缩机制)
                .set("spark.rdd.compress", "true");
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.kryo.registrationRequired", "true")
                //注册自定义类
                //.registerKryoClasses(new Class[]{Registrator.class});
                //.set("spark.kryo.registrator", Registrator.class.getName());
    }

    private static void initSparkSession() {
        if(null == sparkConf) {
            initSparkConf();
        }
        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    private static void initJavaSparkContext() {
        if(null == sparkConf) {
            initSparkConf();
        }
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    public static SparkSession getSparkSession() {
        if(null == sparkSession) {
            initSparkSession();
        }
        return sparkSession;
    }

    public static JavaSparkContext getJavaSparkContext() {
        if(null == javaSparkContext) {
            initJavaSparkContext();
        }
        return javaSparkContext;
    }

    public static void closeSparkSession() {
        if(null != sparkSession) {
            sparkSession.close();
        }
    }

    public static void closeJavaSparkContext() {
        if(null != javaSparkContext) {
            javaSparkContext.close();
        }
    }

    public static void closeSparkSessionAndJavaContext() {
        if(null != sparkSession) {
            sparkSession.close();
        }
        if(null != javaSparkContext) {
            javaSparkContext.close();
        }
    }

}
