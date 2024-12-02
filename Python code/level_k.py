import math
import duckdb

fileName = "deezer_europe_edges_with_scores.csv"
k = 20
line = 5

result = ""
s = ""

duckdb.sql("Create table graph as (select column0 src, column1 dst, column2 score FROM '" + fileName + "')")

n = duckdb.sql("select count(*) from graph").fetchone()[0]

prevGraph = "graph_"+str(line-1)

s = "drop view if exists " + prevGraph + ";"
duckdb.sql(s)
result += s

s = "create view " + prevGraph + " as (select src, dst, score, score maxScore from graph);"
duckdb.sql(s)
result += s

for x in reversed(range(1,line-1)):
    currentGraph = "graph_"+str(x)
    #print("construct " + currentGraph)
    
    s = "drop view if exists " + currentGraph + ";"
    duckdb.sql(s)
    result += s
    
    sq1 = "select src, max(maxScore) accumulatedScore from " + prevGraph + " group by src"
    s = "create view " + currentGraph + " as (with " + prevGraph + "_max as ("+sq1+") select R.src src, R.dst dst, R.score score, R.score+S.accumulatedScore maxScore from graph R, " + prevGraph + "_max S where R.dst=S.src);"
    duckdb.sql(s)
    result += s
    
    # duckdb.sql("select * from "+currentGraph+" order by maxScore desc limit "+str(k)+";").show()
    prevGraph = currentGraph
    pass

s = "drop view if exists graph_0;"
duckdb.sql(s)
result += s

sq1 = "select src, max(maxScore) accumulatedScore from " + prevGraph + " group by src"    
s = "create view graph_0 as (with " + prevGraph + "_max as ("+sq1+") select R.src src, R.dst dst, R.score score, R.score+S.accumulatedScore maxScore from graph R, " + prevGraph + "_max S where R.dst=S.src order by maxScore desc limit "+str(k)+");"
duckdb.sql(s)
result += s

# print("graph_0")
# duckdb.sql("select * from graph_0;").show()

# above works, final result is subset 1st node is subset of graph 0

s = "drop table if exists graph_0_joined;"
duckdb.sql(s)
result += s

prevGraph = "graph_0"
s = "create table graph_0_joined as (select * from graph_0);"
duckdb.sql(s)
result += s
for x in range(1,line):
    currentGraph = "graph_"+str(x)+""
    
    # print("prev join:")
    # duckdb.sql("select * from "+prevGraph+"_joined;").show()

    s = "drop table if exists "+currentGraph+"_id;"
    duckdb.sql(s)
    result += s
    
    sq1 = "select dst, max(score) accumulatedScore, from "+prevGraph+"_joined group by dst"
    sq2 = "select S.src src, S.dst dst, S.score score, S.maxScore maxScore from "+prevGraph+"_joined_max R,"+currentGraph+" S where R.dst=S.src order by R.accumulatedScore+S.maxScore desc limit "+str(k)+""
    s = "create table "+currentGraph+"_id as (with "+prevGraph+"_joined_max as ("+sq1+"), "+currentGraph+"_prune as ("+sq2+") select src, dst, score, maxScore, row_number() over (partition by src order by maxScore desc) as id from "+currentGraph+"_prune);"
    duckdb.sql(s)
    result += s

    s = "create index id_index_"+str(x)+" on "+ currentGraph +"_id (id);"
    duckdb.sql(s)
    result += s

    s = "drop view if exists R0;"
    duckdb.sql(s)
    result += s

    s = "drop view if exists S0;"
    duckdb.sql(s)
    result += s
    
    s = "drop view if exists T0;"
    duckdb.sql(s)
    result += s
    
    s = "create view R0 as (select * from " + prevGraph + "_joined);"
    duckdb.sql(s)
    result += s

    s = "create view S0 as (select * from " +  currentGraph + "_id where id<=2);"
    duckdb.sql(s)
    result += s

    s = "create view T0 as (select R.src src, R.dst joinNode, S.dst dst, S.id id, R.score lscore, lScore+S.score score, lScore+S.maxScore maxScore from R0 R, S0 S where R.dst=S.src order by maxScore desc limit "+str(k)+");"
    duckdb.sql(s)
    result += s

    # print("R0:")
    # duckdb.sql("select * from R0 order by score desc limit "+str(k)+";").show()
    # print("S0:")
    # duckdb.sql("select * from S0 where src=16136 order by score desc limit "+str(k)+";").show()
    # duckdb.sql("select * from "+ currentGraph + "_id where src=16136 order by score desc limit "+str(k)+";").show()
    # print("T0:")
    # duckdb.sql("select * from T0 order by maxScore desc;").show()
    

    for y in range(1,math.ceil(math.log(min(n,k),2))):
        R = "R" + str(y)    
        S = "S" + str(y)
        Tmin1 = "T" + str(y-1)
        T = "T" + str(y)
        s = "drop view if exists " + R + ";"
        duckdb.sql(s)
        result += s
        
        s = "drop view if exists " + S + ";"
        duckdb.sql(s)
        result += s
        
        s = "drop view if exists " + T + ";"
        duckdb.sql(s)
        result += s

        s = "create view " + R + " as(select distinct src, joinNode , lScore score from "+Tmin1+" where id="+str(int(math.pow(2,y)))+");"
        duckdb.sql(s)
        result += s
        
        s = "create view " + S + " as(select * from " +  currentGraph + "_id where id>" + str(int(math.pow(2,y))) + " and id<="+str(int(math.pow(2,y+1))) +");"
        duckdb.sql(s)
        result += s
                
        sq1 = "select R.src src, R.joinNode joinNode, S.dst dst, S.id id, R.score lscore, lScore+ S.score score, lScore+S.maxScore maxScore from " + R + " R, " + S + " S where R.joinNode=S.src order by maxScore desc limit "+str(k)+""
        s = "create view " + T + " as( ("+sq1+") union (select * from "+Tmin1+") order by maxScore desc limit "+str(k)+");"
        duckdb.sql(s)
        
        # print("R:")
        # duckdb.sql("select * from "+R+";").show()
        # print("S:")
        # duckdb.sql("select * from "+S+" where src=16136;").show()
        # print("T:")
        # duckdb.sql("select * from "+T+";").show()

        result += s

    s = "drop table if exists " + currentGraph + "_joined;"
    duckdb.sql(s)
    result += s
    
    s = "create table " + currentGraph + "_joined as (select src, dst, score from " + T +");"
    duckdb.sql(s)
    result += s
    
    prevGraph = currentGraph
    
s = "select * from " + currentGraph + "_joined;"
result += s
print(result)
duckdb.sql(s).show()