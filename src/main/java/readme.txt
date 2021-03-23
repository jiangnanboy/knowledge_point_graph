1 在浏览器http://localhost:7474/browser/
命令行输入以下：
##导入实体
LOAD CSV WITH HEADERS  FROM "file:///zcy.csv" AS line
MERGE (z:中成药{name:line.name})

##导入实体
LOAD CSV WITH HEADERS  FROM "file:///herber.csv" AS line
MERGE (z:中草药{name:line.name})

##导入关系第一种方法：
LOAD CSV WITH HEADERS FROM "file:///r_contain.csv" AS row
match (from:中成药{name:row.from}),(to:中草药{name:row.to})
merge (from)-[r:主要成分{property:row.property}]->(to)

##导入关系第二种方法：
LOAD CSV WITH HEADERS FROM "file:///r_contain.csv" AS line
match (from:中成药{name:line.from}),(to:中草药{name:line.to})
merge (from)-[r:主要成分{property:line.property}]->(to)