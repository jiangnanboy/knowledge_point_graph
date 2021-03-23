package com.sy.dataprocess;

import com.sy.base.abs.AbstractMysqlDataProcess;
import com.sy.util.SparkRead;
import com.sy.util.SparkWrite;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.storage.StorageLevel;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * 处理mysql数据
 * @Author Shi Yan
 * @Date 2020/8/6 14:55
 */
public class MysqlDataProcess extends AbstractMysqlDataProcess {

    private static Logger LOGGER = LoggerFactory.getLogger(MysqlDataProcess.class);

    public MysqlDataProcess(SparkSession session){
        this(session, SparkRead.mysqlReader(session));
    }

    public MysqlDataProcess(SparkSession session, DataFrameReader reader) {
       super(session, reader);
    }

    /**
     * 知识点(knowledge_point_question)，提取实体和关系
     * @param sql
     * @param nodeFileToSave
     * @param relationFileToWrite
     * @param idUUIDToWrite
     * @param nodeHeader
     * @param relationHeader
     */
    public void readTableKG(String sql, String nodeFileToSave, String relationFileToWrite, String idUUIDToWrite, String[] nodeHeader, String[] relationHeader) {
        LOGGER.info("Read knowledge_point_question and extract entity and relationship!");
        Dataset<Row> dataset = mysqlDataRead(sql);
        dataset.persist(StorageLevel.MEMORY_AND_DISK());
        ConcurrentMap<String, String> idMapUUID = new ConcurrentHashMap<>();
        Broadcast<ConcurrentMap<String, String>> broadcastkgIdNameMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(idMapUUID);
        //实体
        JavaRDD<String> nodeJavaRDD = dataset.toJavaRDD().mapPartitions(rowIterator -> {
            List<String> list =  new ArrayList<>();
            StringBuffer sb = null;
            while(rowIterator.hasNext()) {
                sb = new StringBuffer();
                Row row = rowIterator.next();
                String uuid = UUID.randomUUID().toString().replace("-", "");
                broadcastkgIdNameMap.value().put(String.valueOf(row.get(0)), uuid);
                sb.append(uuid).append(",").append(row.getString(1)).append(",").append("KnowledgePoint");
                list.add(sb.toString().trim());
            }
            return list.iterator();
        });
        SparkWrite.writeToOneFile(nodeJavaRDD, nodeFileToSave, nodeHeader);
        //关系
        JavaRDD<String> relationJavaRDD = dataset.filter(col("p_id").gt(0)).toJavaRDD().mapPartitions(rowIterator -> {
            List<String> list = new ArrayList<>();
            StringBuffer sb = null;
            while (rowIterator.hasNext()) {
                sb = new StringBuffer();
                Row row = rowIterator.next();
                if(!broadcastkgIdNameMap.value().containsKey(String.valueOf(row.get(0)))) {
                    continue;
                }
                String id = broadcastkgIdNameMap.value().get(String.valueOf(row.get(0)));
                String p_id = broadcastkgIdNameMap.value().get(String.valueOf(row.get(2)));
                sb.append(id).append(",").append(p_id).append(",").append("BELONG_TO");
                list.add(sb.toString().trim());
            }
            return list.iterator();
        });
        SparkWrite.writeToOneFile(relationJavaRDD, relationFileToWrite, relationHeader);
        dataset.unpersist();
        //id -> uuid
        writeIDMap2UUID(idUUIDToWrite, broadcastkgIdNameMap.value());
    }

    /**
     * 处理题目(question_n)，提取实体
     * @param sql
     * @param questionTypeToRead
     * @param nodeFileToSave
     * @param idUUIDToWrite
     * @param nodeHeader
     */
    public void readTableQuestion(String sql, String questionTypeToRead, String nodeFileToSave, String idUUIDToWrite, String[] nodeHeader) {
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

        LOGGER.info("Read table question and extract entity!");
        Dataset<Row> dataset = mysqlDataRead(sql);
        dataset.persist(StorageLevel.MEMORY_AND_DISK());
        ConcurrentMap<String, String> idMapUUID = new ConcurrentHashMap<>();
        Broadcast<ConcurrentMap<String, String>> broadcastkgIdNameMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(idMapUUID);
        Map<String, String> typeMap = new HashMap<>();
        typeMap.put("0", "混合题型");
        typeMap.put("1", "选择题");
        typeMap.put("2", "判断题");
        typeMap.put("3", "填空题");
        typeMap.put("4", "解答题");
        typeMap.put("5", "作文题");
        Map<String, String> abilityMap = new HashMap<>();
        abilityMap.put("1", "了解和识记");
        abilityMap.put("2", "理解和掌握");
        abilityMap.put("3", "简单应用");
        abilityMap.put("4", "综合应用");
        abilityMap.put("5", "灵活应用");
        Map<String, String> complexityMap = new HashMap<>();
        complexityMap.put("1", "容易");
        complexityMap.put("2", "较易");
        complexityMap.put("3", "一般");
        complexityMap.put("4", "较难");
        complexityMap.put("5", "困难");
        Map<String, String> questionIDMap = readQuestionIDMAPName(questionTypeToRead);
        //实体
        JavaRDD<String> nodeJavaRDD = dataset.toJavaRDD().mapPartitions(rowIterator -> {
            List<String> list =  new ArrayList<>();
            StringBuffer sb = null;
            while(rowIterator.hasNext()) {
                sb = new StringBuffer();
                Row row = rowIterator.next();

                String id = String.valueOf(row.get(0));

                String uuid = String.valueOf(row.get(1)).replace("-", "").trim();
                if((null == uuid) || ("".equals(uuid.trim()))) {
                    continue;
                }

                String type = typeMap.get(String.valueOf(row.get(2)));
                if((null == type) || ("".equals(type.trim()))) {
                    continue;
                }

                String name = Jsoup.parse(row.getString(3)).text().replaceAll(",", "，").replaceAll("\"", "").replaceAll("[\\“]", "");
                if((null == name) || ("".equals(name.trim()))) {
                    continue;
                }

                String question_type = questionIDMap.get(String.valueOf(row.get(4)));
                if((null == question_type) || ("".equals(question_type.trim()))) {
                    continue;
                }

                String answer = "";
                if(StringUtils.isNotBlank(row.getString(5).trim())) {
                    answer = row.getString(5);
                } else if (StringUtils.isNotBlank(row.getString(6).trim())) {
                    answer = Jsoup.parse(row.getString(6)).text().replaceAll(",", "，").replaceAll("\"", "").replaceAll("\\”]","");
                }
                if((null == answer) || ("".equals(answer.trim()))) {
                    continue;
                }

                String parse = Jsoup.parse(row.getString(7)).text().replaceAll(",", "，").replaceAll("\"", "").replaceAll("[\\”]", "");
                if((null == parse) || ("".equals(parse.trim()))) {
                    continue;
                }

                String ability = abilityMap.get(String.valueOf(row.get(8)));
                if((null == ability) || ("".equals(ability.trim()))) {
                    continue;
                }

                String complexity = complexityMap.get(String.valueOf(row.get(9)));
                if((null == complexity) || ("".equals(complexity.trim()))) {
                    continue;
                }

                broadcastkgIdNameMap.value().put(id, uuid);
                sb.append(uuid).append(",")
                        .append(type).append(",")
                        .append(name).append(",")
                        .append(question_type).append(",")
                        .append(answer).append(",")
                        .append(parse).append(",")
                        .append(ability).append(",")
                        .append(complexity).append(",")
                        .append("Question");
                list.add(sb.toString().trim());
            }
            return list.iterator();
        });
        SparkWrite.writeToOneFile(nodeJavaRDD, nodeFileToSave, nodeHeader);
        dataset.unpersist();
        //questionid->uuid
        writeIDMap2UUID(idUUIDToWrite, broadcastkgIdNameMap.value());
    }

    /**
     * 处理question_category_n，提取知识点与题目关系
     * @param sql
     * @param kpID2UUIDPath
     * @param questionID2UUIDPath
     * @param relationFileToWrite
     * @param relationHeader
     */
    public void questionKnowledgePointRelation(String sql, String kpID2UUIDPath, String questionID2UUIDPath, String relationFileToWrite, String[] relationHeader) {
        /**
         * 0 question_id:题目id
         * 1 type:类型 1课标 2知识点 3试卷
         * 2 type_id:根据type显示 课标id,知识点id，试卷id
         * 3 stage 学段
         * 4 course 学科
         */
        LOGGER.info("Read table question_category_n and extract relationship!");
        Dataset<Row> dataset = mysqlDataRead(sql);
        dataset.persist(StorageLevel.MEMORY_AND_DISK());

        ConcurrentMap<String, String> kpId2UUIDMap = readIDMap2UUID(kpID2UUIDPath);//知识点id2uuid
        Broadcast<ConcurrentMap<String, String>> broadcastKpId2UUIDMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(kpId2UUIDMap);

        ConcurrentMap<String, String> questionId2UUIDMap = readIDMap2UUID(questionID2UUIDPath);//题目id2uuid
        Broadcast<ConcurrentMap<String, String>> broadcastQuestionId2UUIDMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(questionId2UUIDMap);

        ConcurrentMap<String, String> courseMap = new ConcurrentHashMap<>();
        courseMap.put("1", "语文");
        courseMap.put("2", "数学");
        courseMap.put("3", "英语");
        courseMap.put("4", "物理");
        courseMap.put("5", "化学");
        courseMap.put("6", "生物");
        courseMap.put("7", "政治");
        courseMap.put("8", "历史");
        courseMap.put("9", "地理");
        courseMap.put("10", "体育");
        courseMap.put("18", "道德与法治");
        courseMap.put("31", "美术");
        courseMap.put("32", "音乐");
        courseMap.put("33", "科学");
        courseMap.put("34", "思品");
        courseMap.put("35", "信息技术");
        courseMap.put("37", "劳技");
        courseMap.put("103", "劳动与技术");
        courseMap.put("116", "通用技术");
        Broadcast<ConcurrentMap<String, String>> broadcastCourseMap = JavaSparkContext.fromSparkContext(session.sparkContext()).broadcast(courseMap);

        JavaRDD<String> javaRDD = dataset.toJavaRDD().mapPartitions(rowIterator -> {
            List<String> list =  new ArrayList<>();
            StringBuffer sb = null;
            while(rowIterator.hasNext()) {
                Row row = rowIterator.next();
                if(!(2 == row.getInt(1))) {
                    continue;
                }
                sb = new StringBuffer();
                String question_uuid = broadcastQuestionId2UUIDMap.value().get(String.valueOf(row.get(0)));//题目id
                String knowledgepoint_uuid = broadcastKpId2UUIDMap.value().get(String.valueOf(row.get(2)));//知识点id
                long stageId = row.getLong(3); // 学段stage
                String stageName = null;
                if(stageId == 1L) {
                    stageName = "小学";
                } else if(stageId == 2L) {
                    stageName = "初中";
                } else if (stageId == 3L) {
                    stageName = "高中";
                }
                String course = broadcastCourseMap.value().get(String.valueOf(row.get(4)));
                if(StringUtils.isNotBlank(question_uuid) && StringUtils.isNotBlank(knowledgepoint_uuid) && StringUtils.isNotBlank(stageName) && StringUtils.isNotBlank(course)) {
                    sb.append(question_uuid).append(",")
                            .append(knowledgepoint_uuid).append(",")
                            .append(stageName).append(",")
                            .append(course).append(",")
                            .append("INCLUDE_A");
                    list.add(sb.toString().trim());
                }
            }
            return list.iterator();
        });
        SparkWrite.writeToOneFile(javaRDD, relationFileToWrite, relationHeader);
        dataset.unpersist();
    }

    /**
     * 读取id与uuid映射数据
     * @param path
     * @return
     */
    public ConcurrentMap<String, String> readIDMap2UUID(String path) {
        ConcurrentMap<String, String> idMapUUID = new ConcurrentHashMap<>();
        Path fPath = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(fPath)) {
            String line = null;
            while((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                idMapUUID.put(strs[0].trim(), strs[1].trim());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return idMapUUID;
    }

    /**
     * question type id -> name
     * @param path
     * @return
     */
    public Map<String, String> readQuestionIDMAPName(String path) {
        Map<String, String> questionTypeIdMap = new HashMap<>();
        Path fPath = Paths.get(path);
        try(BufferedReader br = Files.newBufferedReader(fPath)) {
            String line = null;
            while((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                questionTypeIdMap.put(strs[0].trim(), strs[1].trim());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return questionTypeIdMap;
    }

    /**
     * 保存id与uuid映射数据
     * @param path
     * @param idMapUUID
     */
    public void writeIDMap2UUID(String path, ConcurrentMap<String, String> idMapUUID) {
        Path fPath = Paths.get(path);
        try (BufferedWriter bw = Files.newBufferedWriter(fPath)){
            for(Map.Entry<String, String> entry : idMapUUID.entrySet()) {
                bw.append(entry.getKey()).append(",").append(entry.getValue()).append("\n");
            }
            bw.flush();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
