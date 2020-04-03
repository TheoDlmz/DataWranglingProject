def count(T,target,merging="Classic",verbose=True):
    (R,C,F) = T
    query1 = 'CREATE TABLE count_%s AS SELECT 0 as comp,count(*) as c FROM %s AS R WHERE id NOT IN (SELECT DISTINCT tuple_id FROM %s)' %(target,R,F)
    copy_T = ('copy_R','copy_C','copy_F')    

    sql_exec("DROP TABLE IF EXISTS count_%s"%target)
    if verbose:
        print("[*] Count (%s)..."%target)
    query2 = 'CREATE TABLE copy_C as SELECT * FROM %s' % C    
    query3 = 'CREATE TABLE copy_F as SELECT * FROM %s' % F  
    query4 = 'CREATE TABLE copy_R as SELECT * FROM %s' % R
    if verbose:
        print("[1/3] Creating new tables...")
    sql_exec(query1)
    sql_exec(query2)
    sql_exec(query3)
    sql_exec(query4)
    add_constraints(copy_T,verbose)
    
    
    query_rec = 'SELECT DISTINCT F1.cluster as c1, F2.cluster as c2 FROM copy_F AS F1 JOIN copy_F as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.cluster < F2.cluster'

    if verbose:
        print("[2/3] Merging clusters...")
    if merging == 'Classic':
        merge_clusters(copy_T,query_rec,verbose)
    elif merging == '1by1':
        merge_clusters_one_by_one(copy_T,query_rec,verbose)
    
    strong_normalize(copy_T,verbose)
    
    tuple_lwid = 'SELECT DISTINCT tuple_id,lwid FROM copy_C WHERE (tuple_id,lwid) NOT IN (SELECT DISTINCT tuple_id,lwid FROM copy_C WHERE value is NULL)'
    query5a = 'SELECT DISTINCT cluster,lwid,count(distinct C.tuple_id) as c FROM (%s) as C JOIN copy_F as F ON C.tuple_id = F.tuple_id GROUP BY cluster,lwid' % (tuple_lwid)
    query5 = 'INSERT INTO count_%s SELECT DISTINCT cluster+1 as comp,c FROM (%s) T' % (target,query5a)
    query6 = 'INSERT INTO count_%s SELECT DISTINCT cluster+1 as comp,0 FROM copy_F as F JOIN copy_C as C ON C.tuple_id = F.tuple_id WHERE (cluster,lwid) NOT IN (SELECT cluster,lwid FROM (%s) T)' %(target,query5a)
    if verbose:
        print("[3/3] Add to count...")
    sql_exec(query5)
    sql_exec(query6)
    val_count = 'SELECT sum(c) FROM count_%s WHERE (comp,1) IN (SELECT comp,count(*) as n FROM count_%s GROUP BY comp having n = 1)' % (target,target)
    c = sql(val_count)[0][0]
    query7 = 'UPDATE count_%s SET c = %s WHERE comp = 0'%(target,c)
    sql_exec(query7)
    query8 = 'DELETE FROM count_%s WHERE comp >0 and comp IN (SELECT comp FROM (SELECT comp,count(*) as n FROM count_%s GROUP BY comp having n = 1) T)' % (target,target)
    sql_exec(query8)
    drop_tables(copy_T,verbose)
    
    
    
def sum(T,attribut,target,merging="Classic",verbose=True):
    (R,C,F) = T
    query1 = 'CREATE TABLE sum_%s AS SELECT 0 as comp,sum(%s) as s FROM %s AS R WHERE %s is not NULL' %(target,attribut,R,attribut)
    copy_T = ('copy_R','copy_C','copy_F')    

    sql_exec("DROP TABLE IF EXISTS sum_%s"%target)
    if verbose:
        print("[*] Sum (%s)..."%target)
    query2 = 'CREATE TABLE copy_C as SELECT * FROM %s' % C    
    query3 = 'CREATE TABLE copy_F as SELECT * FROM %s' % F  
    query4 = 'CREATE TABLE copy_R as SELECT * FROM %s' % R
    if verbose:
        print("[1/3] Creating new tables...")
    sql_exec(query1)
    sql_exec(query2)
    sql_exec(query3)
    sql_exec(query4)
    add_constraints(copy_T,verbose)
    
    
    query_rec = 'SELECT DISTINCT F1.cluster as c1, F2.cluster as c2 FROM copy_F AS F1 JOIN copy_F as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.cluster < F2.cluster'

    if verbose:
        print("[2/3] Merging clusters...")
    if merging == 'Classic':
        merge_clusters(copy_T,query_rec,verbose)
    elif merging == '1by1':
        merge_clusters_one_by_one(copy_T,query_rec,verbose)
    
    strong_normalize(copy_T,verbose)
    
    tuple_lwid = 'SELECT tuple_id,lwid,value FROM copy_C WHERE attribut = "%s" and value is not NULL' % attribut
    query5a = 'SELECT DISTINCT cluster,lwid,sum(value) as s FROM (%s) as C JOIN copy_F as F ON C.tuple_id = F.tuple_id WHERE F.attribut ="%s" GROUP BY cluster,lwid' % (tuple_lwid,attribut)
    
    
    query5 = 'INSERT INTO sum_%s SELECT DISTINCT cluster+1 as comp,s FROM (%s) T' % (target,query5a)
    query6 = 'INSERT INTO sum_%s SELECT DISTINCT cluster+1 as comp,0 FROM copy_F as F JOIN copy_C as C ON C.tuple_id = F.tuple_id WHERE (cluster,lwid) NOT IN (SELECT cluster,lwid FROM (%s) T)' %(target,query5a)
    if verbose:
        print("[3/3] Add to count...")
    sql_exec(query5)
    sql_exec(query6)
    val_sum = 'SELECT sum(s) FROM sum_%s WHERE (comp,1) IN (SELECT comp,count(*) as n FROM sum_%s GROUP BY comp having n = 1)' % (target,target)
    s = sql(val_sum)[0][0]
    query7 = 'UPDATE sum_%s SET s = %s WHERE comp = 0'%(target,s)
    sql_exec(query7)
    query8 = 'DELETE FROM sum_%s WHERE comp  >0 and comp IN (SELECT comp FROM (SELECT comp,count(*) as n FROM sum_%s GROUP BY comp having n = 1) T)' % (target,target)
    sql_exec(query8)
    drop_tables(copy_T,verbose)
    
    
    
def avg(T,attribut,target,merging="Classic",verbose=True):
    (R,C,F) = T
    query1 = 'CREATE TABLE avg_%s AS SELECT 0 as comp,count(*) as c,sum(%s)/count(*) as avg FROM %s AS R WHERE %s is not NULL' %(target,attribut,R,attribut)
    copy_T = ('copy_R','copy_C','copy_F')    

    sql_exec("DROP TABLE IF EXISTS avg_%s"%target)
    if verbose:
        print("[*] Average of %s (%s)..."%(attribut,target))
    query2 = 'CREATE TABLE copy_C as SELECT * FROM %s' % C    
    query3 = 'CREATE TABLE copy_F as SELECT * FROM %s' % F  
    query4 = 'CREATE TABLE copy_R as SELECT * FROM %s' % R
    if verbose:
        print("[1/3] Creating new tables...")
    sql_exec(query1)
    sql_exec(query2)
    sql_exec(query3)
    sql_exec(query4)
    add_constraints(copy_T,verbose)
    
    
    query_rec = 'SELECT DISTINCT F1.cluster as c1, F2.cluster as c2 FROM copy_F AS F1 JOIN copy_F as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.cluster < F2.cluster'

    if verbose:
        print("[2/3] Merging clusters...")
    if merging == 'Classic':
        merge_clusters(copy_T,query_rec,verbose)
    elif merging == '1by1':
        merge_clusters_one_by_one(copy_T,query_rec,verbose)
    
    strong_normalize(copy_T,verbose)
    
    tuple_lwid = 'SELECT tuple_id,lwid,value FROM copy_C WHERE attribut = "%s" and value is not NULL' % attribut
    query5a = 'SELECT DISTINCT cluster,lwid,count(distinct F.tuple_id) as c,sum(value) as s FROM (%s) as C JOIN copy_F as F ON C.tuple_id = F.tuple_id WHERE F.attribut ="%s" GROUP BY cluster,lwid' % (tuple_lwid,attribut)
    
    
    query5 = 'INSERT INTO avg_%s SELECT DISTINCT cluster+1 as comp,c,s/c FROM (%s) T' % (target,query5a)
    query6 = 'INSERT INTO avg_%s SELECT DISTINCT cluster+1 as comp,0,0 FROM copy_F as F JOIN copy_C as C ON C.tuple_id = F.tuple_id WHERE (cluster,lwid) NOT IN (SELECT cluster,lwid FROM (%s) T)' %(target,query5a)
    if verbose:
        print("[3/3] Add to count...")
    sql_exec(query5)
    sql_exec(query6)
    val_sum = 'SELECT sum(c),sum(c*avg)/sum(c) FROM avg_%s WHERE (comp,1) IN (SELECT comp,count(*) as n FROM avg_%s GROUP BY comp having n = 1)' % (target,target)
    (c,avg) = sql(val_sum)[0]
    query7 = 'UPDATE avg_%s SET avg = %s,c=%s  WHERE comp = 0'%(target,avg,c)
    sql_exec(query7)
    query8 = 'DELETE FROM avg_%s WHERE comp  >0 and comp IN (SELECT comp FROM (SELECT comp,count(*) as n FROM avg_%s GROUP BY comp having n = 1) T)' % (target,target)
    sql_exec(query8)
    drop_tables(copy_T,verbose)