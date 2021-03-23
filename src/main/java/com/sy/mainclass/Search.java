package com.sy.mainclass;

import com.sy.init.InitNeo4j;
import com.sy.manipulation.basic_operation.GraphSearch;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * @Author Shi Yan
 * @Date 2020/9/17 16:45
 */
public class Search {

    public static void main(String[] args) {
        Driver driver = InitNeo4j.initDriver();
        //getKnowledgePointAndAllFather(driver, "c032ea53c51d437d9a2aea74ed881c77");
        //getCourseAllKnowledgePoint(driver, "67156a7b02834808882c3c8dad966e53");
        //getQuestionIncludeKnowledgePoint(driver, "4f759d9133954469bd27f639b069d227");
        getQuestionIncludeKnowledgePointAndAllFather(driver, "4f759d9133954469bd27f639b069d227");
        InitNeo4j.closeDriver();
    }

    /**
     * 查找某学科的所有知识点
     * @param driver
     * @param nodeId
     */
    public static void getCourseAllKnowledgePoint(Driver driver, String nodeId) {
        GraphSearch graphSearch = new GraphSearch(driver);
        List<Record> listRecord = graphSearch.findCourseAllKnowledgePoint(nodeId);
        getKnowledgePointRelations(listRecord);
    }

    /**
     * 查找该知识点的所有父知识点
     * @param driver
     * @param nodeId
     */
    public static void getKnowledgePointAndAllFather(Driver driver, String nodeId) {
        GraphSearch graphSearch = new GraphSearch(driver);
        List<Record> listRecord = graphSearch.findKnowledgePointAndAllFather(nodeId);
        getKnowledgePointRelations(listRecord);
    }

    /**
     * 查找该题目包含的知识点
     * @param driver
     * @param nodeId
     */
    public static void getQuestionIncludeKnowledgePoint(Driver driver, String nodeId) {
        GraphSearch graphSearch = new GraphSearch(driver);
        List<Record> listRecord = graphSearch.findQuestionIncludeKnowledgePoint(nodeId);
        getQuestionKnowledgePointRelations(listRecord);
    }

    /**
     * 查找该题目包含的知识点及其所有的父知识点
     * @param driver
     * @param nodeId
     */
    public static void getQuestionIncludeKnowledgePointAndAllFather(Driver driver, String nodeId) {
        GraphSearch graphSearch = new GraphSearch(driver);
        List<Record> listRecord = graphSearch.findQuestionIncludeKnowledgePointAndAllFather(nodeId);
        getQuestionKnowledgePointRelations(listRecord);
    }

    /**
     * 提取知识点数据
     * @param listRecord
     */
    private static void getKnowledgePointRelations(List<Record> listRecord) {
        //节点
        Map<String, KnowledgePointNode> kpNodeMap = new HashMap<>(); //知识点节点
        //关系
        Map<String, KnowledgePointRelationship> kpRelationshipMap = new HashMap<>(); //关系：belong_to
        //三元组
        Map<String, Map<String, String>> kgSourceTargetMap = new HashMap<>();//三元线：<sourceid,<targetid,relation>>

        for(Record record : listRecord) {
            // 1.所有节点类入KpNodeMap
            for(Value value : record.get("nodes").values()) {
                String label = value.asNode().labels().iterator().next();
                Map<String, Object> propertyMap = value.asNode().asMap();
                String id = String.valueOf(value.asNode().id()); // 此节点id
                if(kpNodeMap.containsKey(id)) {
                    continue;
                }
                KnowledgePointNode node = new KnowledgePointNode.Builder(label, id).setName(propertyMap.get("name").toString()).build();
                kpNodeMap.put(id, node);
            }

            // 2.所有关系类入kpRelationshipMap；3.所有关系三元组入kgSourceTargetMap
            for(Value value : record.get("relationships").values()) {
                String startID = String.valueOf(value.asRelationship().startNodeId()); // source id
                String endID = String.valueOf(value.asRelationship().endNodeId()); // target id
                String type = value.asRelationship().type(); // 关系 type
                String id = String.valueOf(value.asRelationship().id()); // 关系 id
                KnowledgePointRelationship relationship = new KnowledgePointRelationship.Builder(id, type).build();
                kpRelationshipMap.put(id, relationship);
                Map<String, String> targetRelationMap = null;
                if(kgSourceTargetMap.containsKey(startID)) {
                    targetRelationMap = kgSourceTargetMap.get(startID);
                    if(!targetRelationMap.containsKey(endID)) {
                        targetRelationMap.put(endID, id);
                    }
                } else {
                    targetRelationMap = new HashMap<>();
                    targetRelationMap.put(endID, id);
                    kgSourceTargetMap.put(startID, targetRelationMap);
                }
            }
        }

        //打印
        for(Map.Entry<String, Map<String, String>> entry : kgSourceTargetMap.entrySet()) {
            String sourceId = entry.getKey();
            KnowledgePointNode sourceNode = kpNodeMap.get(sourceId);
            KnowledgePointNode targetNode = null;
            KnowledgePointRelationship relationship = null;
            for(Map.Entry<String, String> map : entry.getValue().entrySet()) {
                targetNode = kpNodeMap.get(map.getKey());
                relationship = kpRelationshipMap.get(map.getValue());
                System.out.println(sourceNode.name + " [" + relationship.getType()+ " ]" + targetNode.name);
            }
        }
    }

    /**
     * 提取题目和知识点
     * @param listRecord
     */
    private static void getQuestionKnowledgePointRelations(List<Record> listRecord) {
        //节点
        Map<String, KnowledgePointNode> kpNodeMap = new HashMap<>(); //知识点节点
        Map<String, QuestionNode> questionNodeMap = new HashMap<>(); //题目节点
        //关系
        Map<String, KnowledgePointRelationship> kpRelationshipMap = new HashMap<>(); //关系 belong_to
        Map<String, QuestionRelationship> questionRelationshipMap = new HashMap<>(); //关系 include_to
        //三元组
        Map<String, Map<String, String>> kpSourceTargetMap = new HashMap<>(); //子知识点属于父知识点关系三元组
        Map<String, Map<String, String>> questionKpSourceTargetMap = new HashMap<>(); //题目包含知识点三元组

        for(Record record : listRecord) {
            // 1.知识点节点入KpNodeMap；题目节点入questionNodeMap
            for(Value value : record.get("nodes").values()) {
                String label = value.asNode().labels().iterator().next();
                Map<String, Object> propertyMap = value.asNode().asMap();
                String id = String.valueOf(value.asNode().id()); // 此节点id
                if(kpNodeMap.containsKey(id) || questionNodeMap.containsKey(id)) {
                    continue;
                }
                if(StringUtils.equals("KnowledgePoint", label)) {
                    KnowledgePointNode node = new KnowledgePointNode.Builder(label, id).setName(propertyMap.get("name").toString()).build();
                    kpNodeMap.put(id, node);
                } else if(StringUtils.equals("Question", label)) {
                    QuestionNode node = new QuestionNode.Builder(label, id).setName(propertyMap.get("name").toString()).build();
                    questionNodeMap.put(id, node);
                }
            }

            // 2.belong_to关系入kpRelationshipMap；include_a关系入questionRelationshipMap 3.所有关系三元组入kgSourceTargetMap
            for(Value value : record.get("relationships").values()) {
                String startID = String.valueOf(value.asRelationship().startNodeId()); // source id
                String endID = String.valueOf(value.asRelationship().endNodeId()); // target id
                String type = value.asRelationship().type(); // 关系 type
                String id = String.valueOf(value.asRelationship().id()); // 关系 id

                if(StringUtils.equals("BELONG_TO", type)) {
                    KnowledgePointRelationship relationship = new KnowledgePointRelationship.Builder(id, type).build();
                    kpRelationshipMap.put(id, relationship);
                    Map<String, String> targetRelationMap = null;
                    if(kpSourceTargetMap.containsKey(startID)) {
                        targetRelationMap = kpSourceTargetMap.get(startID);
                        if(!targetRelationMap.containsKey(endID)) {
                            targetRelationMap.put(endID, id);
                        }
                    } else {
                        targetRelationMap = new HashMap<>();
                        targetRelationMap.put(endID, id);
                        kpSourceTargetMap.put(startID, targetRelationMap);
                    }
                } else if(StringUtils.equals("INCLUDE_A", type)) {
                    Map<String, Object> propertyMap = value.asRelationship().asMap();
                    QuestionRelationship relationship = new QuestionRelationship.Builder(id, type).setCourse(propertyMap.get("course").toString()).setStage(propertyMap.get("stage").toString()).build();
                    questionRelationshipMap.put(id, relationship);
                    Map<String, String> targetRelationMap = null;
                    if(questionKpSourceTargetMap.containsKey(startID)) {
                        targetRelationMap = questionKpSourceTargetMap.get(startID);
                        if(!targetRelationMap.containsKey(endID)) {
                            targetRelationMap.put(endID, id);
                        }
                    } else {
                        targetRelationMap = new HashMap<>();
                        targetRelationMap.put(endID, id);
                        questionKpSourceTargetMap.put(startID, targetRelationMap);
                    }

                }
            }
        }

        //打印知识点[belong_to]三元组
        for(Map.Entry<String, Map<String, String>> entry : kpSourceTargetMap.entrySet()) {
            String sourceId = entry.getKey();
            KnowledgePointNode sourceNode = kpNodeMap.get(sourceId);
            KnowledgePointNode targetNode = null;
            KnowledgePointRelationship relationship = null;
            for(Map.Entry<String, String> map : entry.getValue().entrySet()) {
                targetNode = kpNodeMap.get(map.getKey());
                relationship = kpRelationshipMap.get(map.getValue());
                System.out.println(sourceNode.name + " [ " + relationship.getType()+ " ] " + targetNode.name);
            }
        }

        //打印题目[include_a]三元组
        for(Map.Entry<String, Map<String, String>> entry : questionKpSourceTargetMap.entrySet()) {
            String sourceId = entry.getKey();
            QuestionNode sourceNode = questionNodeMap.get(sourceId);
            KnowledgePointNode targetNode = null;
            QuestionRelationship relationship = null;
            for(Map.Entry<String, String> map : entry.getValue().entrySet()) {
                targetNode = kpNodeMap.get(map.getKey());
                relationship = questionRelationshipMap.get(map.getValue());
                System.out.println(sourceNode.name + " [ " + relationship.getType()+ " ] " + targetNode.name);
            }
        }

    }

    //知识点类
    static class KnowledgePointNode {
        private String label;
        private String id;
        private String name;

        public String getLabel() {
            return label;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public static class Builder {
            private String label;
            private String id;
            private String name = "";

            public Builder(String label, String id) {
                this.label = label;
                this.id = id;
            }
            public KnowledgePointNode build() {
                return new KnowledgePointNode(this);
            }
            public Builder setName(String name) {
                this.name = name;
                return this;
            }
        }

        public KnowledgePointNode(Builder builder) {
            this.label = builder.label;
            this.id = builder.id;
            this.name = builder.name;
        }

        @Override
        public String toString() {
            return "KnowledgePointNode{" +
                    "label='" + label + '\'' +
                    ", id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    //知识点关系类
    static class KnowledgePointRelationship {

        private String id;
        private String type;

        public String getId() { return id;}
        public String getType() {
            return type;
        }

        public static class Builder {
            private String id;
            private String type;

            public Builder(String id, String type) {
                this.id = id;
                this.type = type;
            }
            public KnowledgePointRelationship build() {
                return new KnowledgePointRelationship(this);
            }
        }

        public KnowledgePointRelationship(Builder builder) {
            this.id = builder.id;
            this.type = builder.type;
        }

        @Override
        public String toString() {
            return "KnowledgePointRelationship{" +
                    "id='" + id + '\'' +
                    "type='" + type + '\'' +
                    '}';
        }
    }

    //题目类
    static class QuestionNode {
        private String label;
        private String id;
        private String name;
        private String answer;
        private String parse;
        private String questionType;
        private String type;
        private String complexity;
        private String ability;

        public static class Builder {
            private String label;
            private String id;
            private String name = "";
            private String answer = "";
            private String parse = "";
            private String questionType = "";
            private String type = "";
            private String complexity = "";
            private String ability = "";

            public Builder(String label, String id) {
                this.label = label;
                this.id = id;
            }
            public QuestionNode build() {
                return new QuestionNode(this);
            }
            public Builder setName(String name) {
                this.name = name;
                return this;
            }
            public Builder setAnswer(String answer) {
                this.answer = answer;
                return this;
            }
            public Builder setParse(String parse) {
                this.parse = parse;
                return this;
            }
            public Builder setQuestionType(String questionType) {
                this.questionType = questionType;
                return this;
            }
            public Builder setType(String type) {
                this.type = type;
                return this;
            }
            public Builder setComplexity(String complexity) {
                this.complexity = complexity;
                return this;
            }
            public Builder setAbility(String ability) {
                this.ability = ability;
                return this;
            }
        }

        public QuestionNode(Builder builder) {
            this.label = builder.label;
            this.id = builder.id;
            this.name = builder.name;
            this.answer = builder.answer;
            this.parse = builder.parse;
            this.questionType = builder.questionType;
            this.type = builder.type;
            this.complexity = builder.complexity;
            this.ability = builder.ability;
        }

        @Override
        public String toString() {
            return "QuestionNode{" +
                    "label='" + label + '\'' +
                    ", id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", answer='" + answer + '\'' +
                    ", parse='" + parse + '\'' +
                    ", questionType='" + questionType + '\'' +
                    ", type='" + type + '\'' +
                    ", complexity='" + complexity + '\'' +
                    ", ability='" + ability + '\'' +
                    '}';
        }
    }

    //题目关系类
    static class QuestionRelationship {
        private String id;
        private String type;
        private String course;
        private String stage;

        public String getId() { return id; }

        public String getType() {
            return type;
        }

        public String getCourse() {
            return course;
        }

        public String getStage() {
            return stage;
        }

        public static class Builder {
            private String id;
            private String type;
            private String course = "";
            private String stage = "";

            public Builder(String id, String type) {
                this.id = id;
                this.type = type;
            }

            public Builder setCourse(String course) {
                this.course = course;
                return this;
            }

            public Builder setStage(String stage) {
                this.stage = stage;
                return this;
            }

            public QuestionRelationship build() {
                return new QuestionRelationship(this);
            }
        }

        public QuestionRelationship(Builder builder) {
            this.id = builder.id;
            this.type = builder.type;
            this.course = builder.course;
            this.stage = builder.stage;
        }

        @Override
        public String toString() {
            return "QuestionRelationship{" +
                    "id='" + id + '\'' +
                    "type='" + type + '\'' +
                    "course='" + course + '\'' +
                    "stage='" + stage + '\'' +
                    '}';
        }
    }
}
