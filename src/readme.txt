课标和题目知识点图谱的构建

一.方案：

    1.spark读取mysql相关数据保存为csv格式文件，人工过滤一些不好处理的数据

    2.spark读取csv文件，分别处理成实体和关系文件

    3.neo4j存储数据

二.数据库连接

    1.[mysql.resource_center]

    2.hostname = 196.168.1.5

    3.username = root

    4.password = 123

    5.database = k12_resource

    6.port = 3300

三.表

    1.教材：book

    2.学段：stage

    3.年级：grade

    4.学期：人为设定，分为上学期和下学期

    5.学科：course

    6.单元：来自课标course_standard中的字段description

    7.学科知识点描述：来自课标course_standard中的description

    8.题目：question_N

    9.知识点：knowledge_point_question

    10.题目跟课标、知识点关系：question_category_N

    其中question_N,question_category_N中N与course_id对应

四.清洗数据，定义好实体和关系，图谱的构建分为两部分：

    1.构建教材到课本知识的图谱(根据不同的教材构建课标图谱）

    2.构建题目到知识点的图谱

五.实体(节点)：

    1.教材：book

    2.学段(1小学，2初中，3高中)：stage

    3.年级(一年级到六年级，初一到初三，高一到高三)：grade

    4.学期：semester:上，下

    5.学科：course

    6.单元：unit

    7:教材学科知识点描述：book_course_knowledge_point

    8.题目：question

    9.知识点：knowledge_point

    实体(节点)id的生成格式如下(层层递进，从教材到最终的课本知识描述，便于直观理解)：
        book_N_stage_N_grade_N_semester_N_course_N_unit_N_point_N

六.关系：

    1.(book)-[STAGE]->(stage)

    2.(stage)-[GRADE]->(grade)

    3.(grade)-[SEMESTER]->(semester)

    4.(semester)-[COURSE]->(course)

    5.(course)-[UNIT]->(unit)

    6.(unit)-[HAVE]->(book_course_knowledge_point)

    7.(question)-[INCLUDE]-(knowledge_point)

    8.(question)-[BELONGTO]->(book_course_knowledge_point)

    (KnowledgePoint)-[BELONG_TO]-(KnowledgePoint) 子知识点录属于哪个父知识点，关系为BELONG_TO
    (Question)-[INCLUDE_A]-(KnowledgePoint) 题目拥有的知识点，关系为INCLUDE_A

七.实体(节点)与关系csv数据格式：

    1.以下利用java driver cypher导入数据到db:
        {
        节点：id,name,label
        关系：startId,endId,type
        }

    2.以下格式便于cypher直接导入db：
        {
        movieId:ID,title,year:int,:LABEL
        personId:ID,name,:LABEL
        :START_ID,role,:END_ID,:TYPE
        }


node:knowledge_point_node relationtype:include
node:question relationtype:belong


cypher命名规则：
节点别名用小写驼峰（以小写字母开头）
• 标签用大写驼峰（以大写字母开头）。
• 关系用蛇形大写（类似 IS_A）
• 属性名用 小写驼峰
• 关键词全部用大写