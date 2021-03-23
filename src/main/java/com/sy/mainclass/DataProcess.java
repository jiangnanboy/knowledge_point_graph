package com.sy.mainclass;

import com.sy.dataprocess.MysqlDataProcess;
import com.sy.init.InitSpark;
import com.sy.util.FilePath;
import com.sy.util.SparkWrite;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 读取和处理数据，生成相应的实体和关系数据。
 * @Author Shi Yan
 * @Date 2020/8/13 9:57
 */
public class DataProcess {
    private static Logger logger = LoggerFactory.getLogger(DataProcess.class);

    public static void main(String[] args) {
        SparkSession session = InitSpark.getSparkSession();
        session.sparkContext().setLogLevel("ERROR");
        MysqlDataProcess mysqlDataProcess = new MysqlDataProcess(session);
        //读取知识点
        //readKnowledgePointFromMysql(mysqlDataProcess);
        //读取题目
        readQuestionFromMysql(mysqlDataProcess, "question_2");
        //读取题目和知识点的关联
        readQuestionCategoryFromMysql(mysqlDataProcess, "question_category_2");
        //filterNode(session);
        InitSpark.closeSparkSession();
    }

    /**
     * 知识点knowledge_point_question实体与关系
     * @param mysqlDataProcess
     */
    public static void readKnowledgePointFromMysql(MysqlDataProcess mysqlDataProcess) {
        String sql = "(select id, name, p_id from knowledge_point_question) as KPOINT";
        String[] nodeHeader = {"id", "name", "label"};
        String[] relationHeader = {"startId", "endId", "type"};
        mysqlDataProcess.readTableKG(sql, FilePath.NODE_KNOWLEDGE_POINT_PATH, FilePath.RELATION_BELONGTO_PATH, FilePath.UUID_KPID_PATH, nodeHeader, relationHeader);
    }

    /**
     * 题目 question_n，提取实体
     * @param mysqlDataProcess
     */
    public static void readQuestionFromMysql(MysqlDataProcess mysqlDataProcess, String table) {
        /**
         * 0 id:题目id
         * 1 uuid:题目uuid
         * 2 type:平台题型 0:混合题型 1:选择题 2:判断题 3填空题 4:解答题 5:作文题
         * 3 title:题干
         * 4 question_type:题型
         * 5 answer:答案
         * 6 question_answer:主观题答案
         * 7 parse:解析
         * 8 ability_id:能力要求：1,了解和识记 2,理解和掌握 3,简单应用 4,综合应用 5,灵活应用
         * 9 complexity_id：难易度: 1,容易 2,较易 3,一般 4,较难 5,困难
         */
        String sql = "(select id, uuid, type, title, question_type, answer, question_answer, parse, ability_id, complexity_id from " + table + ") as Question";
        String[] nodeHeader = {"id", "type", "name", "questionType", "answer", "parse", "ability", "complexity", "label"};
        mysqlDataProcess.readTableQuestion(sql, FilePath.QUESTION_TYPE_ID_PATH, FilePath.NODE_QUESTION_PATH, FilePath.UUID_QUESTIONID_PATH, nodeHeader);
    }

    /**
     * 题目关联表 question_category_n，提取题目与知识点的关系
     * @param mysqlDataProcess
     */
    public static void readQuestionCategoryFromMysql(MysqlDataProcess mysqlDataProcess, String table) {
        /**
         * 0 question_id:题目id
         * 1 type:类型 1课标 2知识点 3试卷
         * 2 type_id:根据type显示 课标id,知识点id，试卷id
         * (3.stage 4.course)
         */
        String sql = "(select question_id, type, type_id, stage, course_id from " + table + ") as QC";
        String[] relationHeader = {"startId", "endId", "stage", "course", "type"};
        mysqlDataProcess.questionKnowledgePointRelation(sql, FilePath.UUID_KPID_PATH, FilePath.UUID_QUESTIONID_PATH, FilePath.RELATION_INCLUDE_PATH, relationHeader);
    }

    /**
     * 过滤在关系数据中没有用到的node(避免多创建node数据)
     * @param session
     */
    public static void filterNode(SparkSession session) {
        //1.利用relation数据过滤question node数据
        Set<String> questionSet = new HashSet<>();
        Broadcast<Set<String>> broadcastQuestionSet = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(questionSet);
        JavaRDD<String> relationJavaRDD = session.read().textFile(FilePath.RELATION_INCLUDE_PATH).toJavaRDD();//关系relation
        relationJavaRDD = relationJavaRDD.zipWithIndex().filter(tuple -> tuple._2 > 0).keys().mapPartitions( stringIterator -> {
                List<String> listQuestionId = new ArrayList<>();
                List<String> listString = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String line = stringIterator.next();
                    String questionId = line.split(",")[0].trim();
                    listQuestionId.add(questionId);//question id
                    listString.add(line);
                }
                broadcastQuestionSet.value().addAll(listQuestionId);
                return listString.iterator();
    });
        System.out.println("关系个数："+ relationJavaRDD.count());
        System.out.println("题目个数：" + broadcastQuestionSet.value().size());

        JavaRDD<String> nodeJavaRDD = session.read().textFile(FilePath.NODE_QUESTION_PATH).toJavaRDD();//题目node
        nodeJavaRDD = nodeJavaRDD.zipWithIndex().filter(tuple -> tuple._2 > 0).keys().filter(line -> {
            return broadcastQuestionSet.value().contains(line.split(",")[0].trim());
        });
        System.out.println("过滤后的题目个数：" + nodeJavaRDD.count());

        String[] nodeHeader = {"id", "type", "name", "questionType", "answer", "parse", "ability", "complexity", "label"};
        SparkWrite.writeToOneFile(nodeJavaRDD, FilePath.NODE_PROCESS_PATH, nodeHeader);

        //2.利用question node数据过滤relation 数据
        Set<String> filterQuestionIdSet = new HashSet<>();
        Broadcast<Set<String>> broadcastFilterQuestionSet = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(filterQuestionIdSet);
        nodeJavaRDD = nodeJavaRDD.map(line -> {
            String questionId = line.split(",")[0];
            broadcastFilterQuestionSet.value().add(questionId);
            return line;
        });
        System.out.println("----------------------------");
        System.out.println("过滤后的题目个数：" + nodeJavaRDD.count());
        System.out.println("题目个数：" + broadcastFilterQuestionSet.value().size());
        relationJavaRDD = relationJavaRDD.filter(line -> {
            return broadcastFilterQuestionSet.value().contains(line.split(",")[0]);
        });
        System.out.println("关系个数："+ relationJavaRDD.count());

        String[] relationHeader = {"startId", "endId", "stage", "course", "type"};
        SparkWrite.writeToOneFile(relationJavaRDD, FilePath.RELATION_PROCESS_PATH, relationHeader);

    }

}


