package com.sy.util;

import com.sy.util.PropertiesReader;
import org.apache.derby.catalog.UUID;

/**
 * 文件数据路径
 * @Author Shi Yan
 * @Date 2020/8/21 11:04
 */
public class FilePath {

    public static final String ROW_BOOK_PATH;
    public static final String ROW_STAGE_PATH;
    public static final String ROW_GRADE_PATH;
    public static final String ROW_SEMESTER_PATH;
    public static final String ROW_COURSE_PATH;
    public static final String ROW_COURSE_STANDARD_PATH;
    public static final String ROW_KNOWLEDGE_POINT_PATH;
    public static final String ROW_QUESTION_PATH;
    public static final String ROW＿QUESTION_CATEGORY_PATH;

    public static final String PROCESS_ROW_BOOK_PATH;

    public static final String NODE_BOOK_PATH;
    public static final String NODE_STAGE_PATH;
    public static final String NODE_GRADE_PATH;
    public static final String NODE_SEMESTER_PATH;
    public static final String NODE_COURSE_PATH;
    public static final String NODE_UNIT_PATH;
    public static final String NODE_BOOK_COURSE_KNOWLEDGE_POINT_PATH;
    public static final String NODE_QUESTION_PATH;
    public static final String NODE_KNOWLEDGE_POINT_PATH;
    public static final String NODE_PROCESS_PATH;

    public static final String RELATION_BELONGTO_PATH;
    public static final String RELATION_COURSE_PATH;
    public static final String RELATION_GRADE_PATH;
    public static final String RELATION_HAVE_PATH;
    public static final String RELATION_INCLUDE_PATH;
    public static final String RELATION_SEMESTER_PATH;
    public static final String RELATION_STAGE_PATH;
    public static final String RELATION_UNIT_PATH;
    public static final String RELATION_PROCESS_PATH;

    public static final String UUID_KPID_PATH;
    public static final String UUID_QUESTIONID_PATH;
    public static final String QUESTION_TYPE_ID_PATH;

    static {

        /**
         * row data path
         */
        ROW_BOOK_PATH = PropertiesReader.get("row_book");
        ROW_STAGE_PATH = PropertiesReader.get("row_stage");
        ROW_GRADE_PATH = PropertiesReader.get("row_grade");
        ROW_SEMESTER_PATH = PropertiesReader.get("row_semester");
        ROW_COURSE_PATH = PropertiesReader.get("row_course");
        ROW_COURSE_STANDARD_PATH = PropertiesReader.get("row_course_standard");
        ROW_KNOWLEDGE_POINT_PATH = PropertiesReader.get("row_knowledge_point");
        ROW_QUESTION_PATH = PropertiesReader.get("row_question");
        ROW＿QUESTION_CATEGORY_PATH = PropertiesReader.get("row_question_category");

        /**
         * process row data path
         */
        PROCESS_ROW_BOOK_PATH = PropertiesReader.get("process_row_book");

        /**
         * node data path
         */
        NODE_BOOK_PATH = PropertiesReader.get("node_book");
        NODE_STAGE_PATH = PropertiesReader.get("node_stage");
        NODE_GRADE_PATH = PropertiesReader.get("node_grade");
        NODE_SEMESTER_PATH = PropertiesReader.get("node_semester");
        NODE_COURSE_PATH = PropertiesReader.get("node_course");
        NODE_UNIT_PATH = PropertiesReader.get("node_unit");
        NODE_BOOK_COURSE_KNOWLEDGE_POINT_PATH = PropertiesReader.get("node_book_course_knowledge_point");
        NODE_QUESTION_PATH = PropertiesReader.get("node_question");
        NODE_KNOWLEDGE_POINT_PATH = PropertiesReader.get("node_knowledge_point");
        NODE_PROCESS_PATH = PropertiesReader.get("node_process");

        /**
         * relation data path
         */
        RELATION_BELONGTO_PATH = PropertiesReader.get("relation_belongto");
        RELATION_COURSE_PATH = PropertiesReader.get("relation_course");
        RELATION_GRADE_PATH = PropertiesReader.get("relation_grade");
        RELATION_HAVE_PATH = PropertiesReader.get("relation_have");
        RELATION_INCLUDE_PATH = PropertiesReader.get("relation_include");
        RELATION_SEMESTER_PATH = PropertiesReader.get("relation_semester");
        RELATION_STAGE_PATH = PropertiesReader.get("relation_stage");
        RELATION_UNIT_PATH = PropertiesReader.get("relation_unit");
        RELATION_PROCESS_PATH = PropertiesReader.get("relation_process");

        /**
         * knowledge point id map2 uuid
         */
        UUID_KPID_PATH = PropertiesReader.get("uuid_kpid");
        UUID_QUESTIONID_PATH = PropertiesReader.get("uuid_questionid");
        QUESTION_TYPE_ID_PATH = PropertiesReader.get("question_type_id");
    }

}
