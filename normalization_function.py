import numpy as np
import time
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

R0 = ("arbres_0","C","F")

##

def sql(query):
    #print(query)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    return mycursor.fetchall()

def sql_exec(query):
    #print(query)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    #print(mycursor.rowcount, "record inserted.")
    

def sql_exec_list(query_list):
    #print(query)
    mycursor = mydb.cursor()
    for query in query_list:
        mycursor.execute(query)
    mydb.commit()
    #print(mycursor.rowcount, "record inserted.")


##    
    
def tabname(name):
    return ("R_%s"%name,"C_%s"%name,"F_%s"%name)


def drop_tables(T,verbose=True):
    (R,C,F) = T
    if verbose:
        print("[1/1] Deleting old tables...")
    sql_exec("DROP TABLE IF EXISTS %s"%R)    
    sql_exec("DROP TABLE IF EXISTS %s"%C)    
    sql_exec("DROP TABLE IF EXISTS %s"%F)
    
    
def add_constraints(T,verbose=True):
    (R,C,F) = T
    if verbose:
        print("[1/1] Adding constraints...")
    sql_exec("ALTER TABLE %s ADD PRIMARY KEY (id)"%R)
    sql_exec("ALTER TABLE %s ADD PRIMARY KEY (tuple_id,attribut,lwid)"%C)
    sql_exec("ALTER TABLE %s ADD PRIMARY KEY (tuple_id,attribut)"%F)
    

def removeWhenOne(T,verbose=True):
    (R,C,F) = T
    select_query = 'SELECT DISTINCT C.tuple_id,C.attribut,C.value FROM %s as C JOIN (SELECT tuple_id,attribut,count(distinct value) as nbv from %s group by tuple_id, attribut having nbv <= 1 AND (tuple_id,attribut) not in (select tuple_id,attribut from %s WHERE value is NULL)) as T ON C.tuple_id = T.tuple_id AND C.attribut = T.attribut' % (C,C,C)
    if verbose:
        print("[1/3] Selecting values...")
    elem_to_update = sql(select_query)
    if verbose:
        print("[2/3] Updating R...")
    for (tuple_id,attribut,value) in elem_to_update:
        sql_exec('UPDATE %s SET %s = "%s" WHERE id = %s' % (R,attribut,value,tuple_id))
    if verbose:
        print("[3/3] Delete duplicates in C and F...")
    delete_query_F = 'DELETE FROM %s WHERE (tuple_id,attribut) in (SELECT tuple_id,attribut from (%s) as T)' % (F,select_query)
    delete_query_C = 'DELETE FROM %s WHERE (tuple_id,attribut) in (SELECT tuple_id,attribut from (%s) as T)' % (C,select_query)
    sql_exec(delete_query_F)
    sql_exec(delete_query_C)
    
def reindex(T,verbose=True):
    (R,C,F) =T
    sql_exec("DROP TABLE IF EXISTS Ctemp")
    
    query_toreindex = 'CREATE TABLE toreindex AS SELECT DISTINCT tuple_id,attribut FROM %s WHERE lwid >0 AND (tuple_id,attribut,lwid) NOT IN (SELECT tuple_id,attribut,lwid+1 FROM %s)' % (C,C)
    query_constraint0 = 'ALTER TABLE toreindex ADD KEY (tuple_id,attribut)'
    query_normalize = 'CREATE TABLE Ctemp AS SELECT C1.tuple_id as tuple_id,C1.attribut as attribut,count(C2.lwid)-1 as lwid,C1.value as value FROM %s as C1 JOIN %s as C2 JOIN toreindex as T ON T.tuple_id = C1.tuple_id AND T.attribut  = C1.attribut AND C1.tuple_id = C2.tuple_id AND C1.attribut = C2.attribut WHERE C2.lwid <= C1.lwid GROUP BY C1.tuple_id,C1.attribut,C1.lwid' % (C,C)
    query_normalize2 = 'INSERT INTO Ctemp SELECT * FROM %s as C WHERE (tuple_id,attribut) not in (select * from toreindex)' % C
    query_delete = 'DROP TABLE %s ' %C
    query_delete2 = 'DROP TABLE toreindex'
    query_contraint = 'ALTER TABLE Ctemp ADD KEY(tuple_id,attribut,lwid,value)'
    query_insert = 'CREATE TABLE %s as SELECT * FROM Ctemp'% C
    query_drop = ' DROP TABLE Ctemp'   
    query_contraint2 = 'ALTER TABLE %s ADD PRIMARY KEY(tuple_id,attribut,lwid)'% C
    
    
    if verbose: 
        print("[1/4] Re-indexing...")
    sql_exec(query_toreindex)    
    sql_exec(query_constraint0)

    if verbose: 
        print("[2/4] Re-indexing...")
    sql_exec(query_normalize)    
    sql_exec(query_normalize2)    
    if verbose: 
        print("[3/4] Re-indexing...")
    sql_exec(query_delete)
    sql_exec(query_delete2)
    sql_exec(query_contraint)
    if verbose: 
        print("[4/4] Re-indexing...")
    sql_exec(query_insert)
    sql_exec(query_drop)
    sql_exec(query_contraint2)
    
def propagate_null(T,verbose=True):
    (R,C,F) = T

    query_propagate = 'REPLACE INTO %s SELECT C1.tuple_id,C1.attribut,C1.lwid,NULL FROM %s as C1 JOIN %s as F1 JOIN %s as F2 JOIN %s as C2 ON C1.tuple_id = F1.tuple_id AND C1.attribut = F1.attribut AND C1.tuple_id = C2.tuple_id and C2.tuple_id = F2.tuple_id and C2.attribut = F2.attribut and F1.cluster = F2.cluster and C1.lwid = C2.lwid WHERE C1.value is not NULL and C2.value is NULL' % (C,C,F,F,C)
    
    if verbose:
        print("[1/1] Propagate null...")
    sql_exec(query_propagate)
    
def remove_duplicate(T,verbose=True):
    (R,C,F) = T
    
    table1 = 'CREATE TABLE normal1 AS SELECT DISTINCT cluster,lwid FROM  %s as C JOIN %s as F ON F.tuple_id = C.tuple_id AND F.attribut = C.attribut WHERE value is not NULL ' % (C,F)
    query_minbad = 'SELECT DISTINCT C.tuple_id, C.attribut, min(C.lwid) as lwid FROM %s as C JOIN %s AS F ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE (cluster,lwid) NOT IN (SELECT * FROM normal1) GROUP BY C.tuple_id, C.attribut' % (C,F)
    query_good = 'SELECT DISTINCT C.tuple_id, C.attribut, lwid FROM %s as C JOIN %s AS F ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE (cluster,lwid) IN (SELECT * FROM normal1)' % (C,F)
    create1 = 'CREATE TABLE C_temp AS SELECT DISTINCT C.tuple_id,C.attribut,C.lwid,C.value FROM (%s UNION %s) as T JOIN %s as C ON C.tuple_id = T.tuple_id AND C.attribut = T.attribut AND C.lwid = T.lwid' % (query_minbad,query_good,C)
    droptable1 = 'DROP TABLE %s' % C
    create2 = 'CREATE TABLE %s as SELECT * FROM C_temp'%C
    constraint = "ALTER TABLE %s ADD PRIMARY KEY(tuple_id,attribut,lwid)"%C
    droptable2 = 'DROP TABLE C_temp'
    droptable3 = 'DROP TABLE normal1'
    
    if verbose:
        print("[1/3] Deleting tuples with Null...")
    sql_exec(table1)
    
    if verbose:
        print("[2/3] Deleting tuples with Null...")
    sql_exec(create1)
    sql_exec(droptable1)
    
    if verbose:
        print("[3/3] Deleting tuples with Null...")
    sql_exec(create2)
    sql_exec(droptable2)
    sql_exec(droptable3)
    sql_exec(constraint)


def weak_normalize(T,verbose=True):
    if verbose:
        print("[*] Weak normalization...")
    propagate_null(T,verbose)
    
def strong_normalize(T,remove_when_one=False,verbose=True):
    if verbose:
        print("[*] Strong normalization...")
    propagate_null(T,verbose)
    remove_duplicate(T,verbose)
    reindex(T,verbose)
    if remove_when_one:
        removeWhenOne(T,verbose)

## MERGE FUNCTION
    
def merge_clusters(T,query,verbose=True):
    if verbose:
        print("[*] Mergeing clusters...")
    (R,C,F) = T
    step = 1
    clusters_to_merge = 1
    while clusters_to_merge > 0:
        query1 = "CREATE TABLE close_clusters AS %s"%query
        query2 = 'WITH RECURSIVE clusters(c1,c2) AS (SELECT * FROM close_clusters UNION ALL SELECT clust1.c1,clust2.c2 FROM clusters as clust1 JOIN close_clusters as clust2 ON clust1.c2 = clust2.c1) SELECT min(c1) as c1,c2 FROM (SELECT c1,max(c2) as c2 FROM clusters GROUP BY c1) as T group by c2'
        query3 = "CREATE TABLE clusters2merge AS %s"%query2
        query4 = "DROP TABLE close_clusters"
        countquery3 = "SELECT COUNT(*) FROM clusters2merge"
        if verbose:
            print("[%s-1/8] Mergeing : find clusters to merge"%step)
        sql_exec(query1)
        sql_exec(query3)
        sql_exec(query4)
        clusters_to_merge = sql(countquery3)[0][0]

        
        if verbose:
            print("[%s-2/8] Mergeing : %s clusters found"%(step,clusters_to_merge))
            
        if clusters_to_merge == 0:
            query16 = 'DROP TABLE clusters2merge'
            sql_exec(query16)
            break
            
        query5 = 'CREATE TABLE merge1 AS SELECT C.tuple_id,C.attribut,C.lwid*T.lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT FT.cluster as c2,max(lwid)+1 as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut GROUP BY c2) as T ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE (F.cluster,T.c2) in (SELECT * FROM clusters2merge)'% (C,F,C,F)
        query6 = 'ALTER TABLE merge1 ADD KEY(tuple_id,attribut,lwid)'
        query7 = 'DELETE FROM %s WHERE (tuple_id,attribut) in (SELECT tuple_id,attribut FROM merge1)' % C
        query8 = 'INSERT INTO %s SELECT * FROM merge1' % C
        query9 = 'DROP TABLE merge1'
        
        if verbose:
            print("[%s-3/8] Mergeing : creating new lwid"%(step))
        sql_exec(query5)
        sql_exec(query6)
        sql_exec(query7)
        
        if verbose:
            print("[%s-4/8] Mergeing : inserting new lwid"%(step))
        sql_exec(query8)
        sql_exec(query9)
        
        query10 = 'CREATE TABLE merge2 AS SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT FT.cluster as c2,lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut AND lwid > 0) as T JOIN clusters2merge M ON M.c2 = T.c2 AND M.c1 = F.cluster  AND C.tuple_id = F.tuple_id AND C.attribut = F.attribut' % (C,F,C,F)
        query11 = 'ALTER TABLE merge2 ADD KEY(tuple_id,attribut,lwid)'
        query12 = 'INSERT INTO %s SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT FT.cluster as c2,lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut WHERE lwid > 0) as T JOIN clusters2merge as M ON M.c1 = T.c2 AND M.c2 = F.cluster AND C.tuple_id = F.tuple_id AND C.attribut = F.attribut ' % (C,C,F,C,F)
        query13 = 'INSERT INTO %s SELECT * FROM merge2' % (C)
        query14 = 'DROP TABLE merge2'
        query15 = 'REPLACE INTO %s SELECT F1.tuple_id,F1.attribut,F2.c2 FROM %s as F1 JOIN clusters2merge as F2 ON F1.cluster = F2.c1' %(F,F)
        query16 = 'DROP TABLE clusters2merge'
        
        if verbose:
            print("[%s-5/8] Mergeing : inserting new lwid"%(step))
        sql_exec(query10)
        sql_exec(query11)
        
        if verbose:
            print("[%s-6/8] Mergeing : inserting new lwid"%(step))
        sql_exec(query12)
        
        if verbose:
            print("[%s-7/8] Mergeing : inserting new lwid"%(step))
        sql_exec(query13)
        sql_exec(query14)
        
        if verbose:
            print("[%s-8/8] Mergeing : changing cluster values"%(step))
        sql_exec(query15)
        sql_exec(query16)
        step +=1
        
        
def merge_clusters_selection(T,query,attribut1,attribut2,operateur,verbose=True):
    if verbose:
        print("[*] Mergeing clusters + selection...")
    (R,C,F) = T
    step = 1
    clusters_to_merge = 1
    while clusters_to_merge > 0:
        query1 = "CREATE TABLE close_clusters AS %s"%query
        query2 = 'WITH RECURSIVE clusters(c1,c2) AS (SELECT * FROM close_clusters UNION ALL SELECT clust1.c1,clust2.c2 FROM clusters as clust1 JOIN close_clusters as clust2 ON clust1.c2 = clust2.c1) SELECT min(c1) as c1,c2 FROM (SELECT c1,max(c2) as c2 FROM clusters GROUP BY c1) as T group by c2'
        query3 = "CREATE TABLE clusters2merge AS %s"%query2
        query4 = "DROP TABLE close_clusters"
        countquery3 = "SELECT COUNT(*) FROM clusters2merge"
        if verbose:
            print("[%s-1/9] Mergeing : find clusters to merge"%step)
        sql_exec(query1)
        sql_exec(query3)
        sql_exec(query4)
        clusters_to_merge = sql(countquery3)[0][0]

        
        if verbose:
            print("[%s-2/9] Mergeing : %s clusters found"%(step,clusters_to_merge))
            
        if clusters_to_merge == 0:
            query16 = 'DROP TABLE clusters2merge'
            sql_exec(query16)
            break
            
        query5 = 'CREATE TABLE merge1 AS SELECT C.tuple_id,C.attribut,C.lwid*T.lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT FT.cluster as c2,max(lwid)+1 as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut GROUP BY c2) as T ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE (F.cluster,T.c2) in (SELECT * FROM clusters2merge)'% (C,F,C,F)
        query6 = 'ALTER TABLE merge1 ADD KEY(tuple_id,attribut,lwid)'
        query7 = 'DELETE FROM %s WHERE (tuple_id,attribut) in (SELECT tuple_id,attribut FROM merge1)' % C
        query8 = 'INSERT INTO %s SELECT * FROM merge1' % C
        query9 = 'DROP TABLE merge1'
        
        if verbose:
            print("[%s-3/9] Mergeing : creating new lwid"%(step))
        sql_exec(query5)
        sql_exec(query6)
        sql_exec(query7)
        
        if verbose:
            print("[%s-4/9] Mergeing : inserting new lwid"%(step))
        sql_exec(query8)
        sql_exec(query9)
        
        query10 = 'CREATE TABLE merge2 AS SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT FT.cluster as c2,lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut AND lwid > 0) as T JOIN clusters2merge M ON M.c2 = T.c2 AND M.c1 = F.cluster  AND C.tuple_id = F.tuple_id AND C.attribut = F.attribut' % (C,F,C,F)
        query11 = 'ALTER TABLE merge2 ADD KEY(tuple_id,attribut,lwid)'
        query12 = 'INSERT INTO %s SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT FT.cluster as c2,lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut WHERE lwid > 0) as T JOIN clusters2merge as M ON M.c1 = T.c2 AND M.c2 = F.cluster AND C.tuple_id = F.tuple_id AND C.attribut = F.attribut ' % (C,C,F,C,F)
        query13 = 'INSERT INTO %s SELECT * FROM merge2' % (C)
        query14 = 'DROP TABLE merge2'
        query15 = 'REPLACE INTO %s SELECT F1.tuple_id,F1.attribut,F2.c2 FROM %s as F1 JOIN clusters2merge as F2 ON F1.cluster = F2.c1' %(F,F)
        query16 = 'DROP TABLE clusters2merge'
        
        if verbose:
            print("[%s-5/9] Mergeing : inserting new lwid"%(step))
        sql_exec(query10)
        sql_exec(query11)
        
        if verbose:
            print("[%s-6/9] Mergeing : inserting new lwid"%(step))
        sql_exec(query12)
        
        if verbose:
            print("[%s-7/9] Mergeing : inserting new lwid"%(step))
        sql_exec(query13)
        sql_exec(query14)
        
        if verbose:
            print("[%s-8/9] Mergeing : changing cluster values"%(step))
        sql_exec(query15)
        sql_exec(query16)
        
        query17a = 'SELECT C1.tuple_id,C1.lwid FROM %s as C1 JOIN %s as C2 ON C1.tuple_id = C2.tuple_id AND C1.lwid = C2.lwid WHERE C1.attribut = "%s" AND C2.attribut = "%s" AND not(C1.value %s C2.value) ' % (C,C,attribut1,attribut2,operateur)
        query17 = 'REPLACE INTO %s SELECT C.tuple_id,C.attribut,C.lwid,NULL FROM %s as C JOIN (%s) as T ON C.tuple_id = T.tuple_id AND C.lwid = T.lwid WHERE C.attribut = "%s" OR C.attribut = "%s"' % (C,C,query17a,attribut1,attribut2)
        
        
        if verbose:
            print("[%s-9/9] Selecting tuples + normalize..."%step)
        
        sql_exec(query17)
        strong_normalize(T,verbose)
        
        step +=1
        
def merge_clusters_one_by_one(T,query,verbose=True):
    if verbose:
        print("[*] Mergeing clusters 1 by 1...")
    (R,C,F) = T
    
    close_clusters = sql(query)
    n_clust = len(close_clusters)
    for i in range(n_clust):
        (a,b) = close_clusters[i]
        if a == b:
            continue
        if verbose:
            print("{%s/%s} Merging %s and %s" %(i+1,n_clust,a,b))
        
            
        query5 = 'CREATE TABLE merge1 AS SELECT C.tuple_id,C.attribut,C.lwid*T.lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT max(lwid)+1 as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut where cluster = %s) as T ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE F.cluster = %s'% (C,F,C,F,a,b)
        query6 = 'ALTER TABLE merge1 ADD KEY(tuple_id,attribut,lwid)'
        query7 = 'DELETE FROM %s WHERE (tuple_id,attribut) in (SELECT tuple_id,attribut FROM merge1)' % C
        query8 = 'INSERT INTO %s SELECT * FROM merge1' % C
        query9 = 'DROP TABLE merge1'
        
        if verbose:
            print("[%s-1] Mergeing : creating new lwid"%(i+1))
        sql_exec(query5)
        sql_exec(query6)
        sql_exec(query7)
        
        if verbose:
            print("[%s-2] Mergeing : inserting new lwid"%(i+1))
        sql_exec(query8)
        sql_exec(query9)
        
        query10 = 'CREATE TABLE merge2 AS SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut WHERE lwid > 0 AND FT.cluster = %s) as T ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE F.cluster = %s' % (C,F,C,F,a,b)
        query11 = 'ALTER TABLE merge2 ADD KEY(tuple_id,attribut,lwid)'
        query12 = 'INSERT INTO %s SELECT C.tuple_id,C.attribut,C.lwid+lwid2 as lwid,C.value FROM %s as C JOIN %s as F JOIN (SELECT DISTINCT FT.cluster as c2,lwid as lwid2 FROM %s as CT JOIN %s AS FT ON CT.tuple_id = FT.tuple_id AND CT.attribut = FT.attribut WHERE lwid > 0 AND FT.cluster = %s) as T ON C.tuple_id = F.tuple_id AND C.attribut = F.attribut WHERE F.cluster = %s ' % (C,C,F,C,F,b,a)
        query13 = 'INSERT INTO %s SELECT * FROM merge2' % (C)
        query14 = 'DROP TABLE merge2'
        query15 = 'REPLACE INTO %s SELECT tuple_id,attribut,%s FROM %s WHERE cluster = %s' %(F,a,F,b)
        
        if verbose:
            print("[%s-3] Mergeing : inserting new lwid"%(i+1))
        sql_exec(query10)
        sql_exec(query11)
        
        if verbose:
            print("[%s-4] Mergeing : inserting new lwid"%(i+1))
        sql_exec(query12)
        
        if verbose:
            print("[%s-5] Mergeing : inserting new lwid"%(i+1))
        sql_exec(query13)
        sql_exec(query14)
        
        if verbose:
            print("[%s-6] Mergeing : changing cluster values"%(i+1))
        sql_exec(query15)
        
        if verbose:
            print("[%s-7] Mergeing : python"%(i+1))
            
        for j in range(i+1,n_clust):
            if close_clusters[j][0] == b:
                close_clusters[j] = (a,close_clusters[j][1])

            if close_clusters[j][1] == b:
                close_clusters[j] = (close_clusters[j][0],a)