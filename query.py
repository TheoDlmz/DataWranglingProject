# Do a mathematical operation on one particular attribute

def operation_one(T,attribut,operation,new_attribut,target,verbose=True):
    # operationg attribut operationd as new_attribut
    (R,C,F) = T
    if verbose:
       print("[*] Operation %s as %s (%s)"%(operation%attribut,new_attribut,target))
    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    
    
    query1 = 'CREATE TABLE C_%s AS SELECT * FROM %s WHERE attribut <> "%s"' % (target,C,attribut)
    query2 = 'INSERT INTO C_%s SELECT tuple_id,"%s",lwid, %s FROM %s WHERE attribut = "%s"' % (target,new_attribut,operation%"value",C,attribut)
    query3 = 'CREATE TABLE F_%s AS SELECT * FROM %s WHERE attribut <> "%s"' % (target,F,attribut)
    query4 = 'INSERT INTO F_%s SELECT tuple_id,"%s",cluster FROM %s WHERE attribut = "%s"' % (target,new_attribut,F,attribut)
    query5 = 'CREATE TABLE R_%s AS SELECT R.*,%s as %s FROM %s as R ' % (target,operation%attribut,new_attribut,R)
    
    if verbose:
        print("[1/3] Creating new C...")
    sql_exec(query1)
    sql_exec(query2)
    
    if verbose:
       print("[2/3] Creating new F...")
    sql_exec(query3)
    sql_exec(query4)
    
    if verbose:
       print("[3/3] Creating new R...")
    sql_exec(query5)
    
    add_constraints(newtab,verbose)
    return newtab
    
  

def rename(T,attribut_maps,target,verbose=True):
    (R,C,F) = T
    
    if verbose:
       print("[*] Renaming (%s)"%(target))
    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    query1 = 'CREATE TABLE attributs_%s (old_att VARCHAR(255), new_att VARCHAR(255))'%target
    query2 = 'CREATE TABLE R_%s AS SELECT * FROM %s' % (target,R)
    
    if verbose:
        print("[1/3] Creating new R...")
    sql_exec(query1)
    sql_exec(query2)
    for (old_att,new_att) in attribut_maps:
        query3a = 'INSERT INTO attributs_%s VALUE ("%s","%s")'%(target,old_att,new_att)
        query3b= 'ALTER TABLE R_%s RENAME COLUMN %s TO %s'%(target,old_att,new_att)
        sql_exec(query3a)
        sql_exec(query3b)
    
    if verbose:
        print("[2/3] Creating new C...")
    query4 = 'CREATE TABLE C_%s AS SELECT tuple_id,A.new_att as attribut,lwid,value FROM %s as C JOIN attributs_%s as A ON C.attribut = A.old_att '%(target,C,target)
    sql_exec(query4)
    
    if verbose:
        print("[3/3] Creating new F...")
    query5 = 'CREATE TABLE F_%s AS SELECT tuple_id,A.new_att as attribut,cluster FROM %s as F JOIN attributs_%s as A ON F.attribut = A.old_att '%(target,F,target)
    query6 = "DROP TABLE attributs_%s"%target
    sql_exec(query5)
    sql_exec(query6)
    
    add_constraints(newtab,verbose)
    return newtab
    
    
    
    
def cross_product(T1,T2,target,verbose=True):
    (R1,C1,F1) = T1
    (R2,C2,F2) = T2
    if verbose:
        print("[*] Cross product (%s)"%target)    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    
    r1att = 'R1.'+',R1.'.join(list(np.array(sql("SHOW columns FROM %s"%R1))[1:,0]))
    r2att = 'R2.' + ',R2.'.join(list(np.array(sql("SHOW columns FROM %s"%R2))[1:,0]))
    query1 = 'CREATE TABLE new_id AS SELECT R1.id as id_1,R2.id as id_2 FROM %s as R1, %s as R2' % (R1,R2)
    query2a = 'ALTER TABLE new_id ADD COLUMN id INT PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST'
    query2b = 'ALTER TABLE new_id ADD KEY(id_1,id_2)'

    query9a = 'CREATE TABLE new_clusters AS SELECT F.cluster as id_1,1 as id_2 FROM %s as F UNION SELECT F.cluster as id_1,2 as id_2 FROM %s as F' % (F1,F2)
    query9b = 'ALTER TABLE new_clusters ADD COLUMN id INT PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST'
    query9c = 'ALTER TABLE new_clusters ADD KEY(id_1,id_2)'
    
    query3 = 'CREATE TABLE C_%s AS SELECT I.id as tuple_id,C.attribut,C.lwid,C.value FROM %s AS C JOIN new_id I ON I.id_1 = C.tuple_id' %(target,C1)
    
    query4 ='INSERT INTO C_%s SELECT I.id as tuple_id,C.attribut,C.lwid,C.value FROM %s AS C JOIN new_id I ON I.id_2 = C.tuple_id' %(target,C2)
    
    query5 ='CREATE TABLE F_%s AS SELECT I.id as tuple_id,F.attribut,C.id as cluster FROM %s AS F JOIN new_id I JOIN new_clusters as C ON I.id_1 = F.tuple_id AND C.id_1 = F.cluster WHERE C.id_2 = 1' %(target,F1)
    
    query6 ='INSERT INTO F_%s SELECT I.id as tuple_id,F.attribut,C.id FROM %s AS F JOIN new_id I JOIN new_clusters as C ON I.id_2 = F.tuple_id  AND C.id_1 = F.cluster WHERE C.id_2 = 2' %(target,F2)
    
    query7 ='CREATE TABLE R_%s AS SELECT I.id,%s,%s FROM new_id as I JOIN %s as R1 JOIN %s as R2 ON I.id_1 = R1.id AND I.id_2 = R2.id order by I.id' % (target,r1att,r2att,R1,R2)
    
    query8 ='DROP TABLE new_id'
    query9d = 'DROP TABLE new_clusters'
    if verbose:
      print("[1/5] Create new ids...")
    sql_exec(query1)    
    sql_exec(query2a)
    sql_exec(query2b)
    if verbose:
      print("[2/5] Create new clusters...")
    sql_exec(query9a)
    sql_exec(query9b)
    sql_exec(query9c)
    if verbose:
      print("[3/5] Creating new C...")  
    sql_exec(query3)    
    sql_exec(query4)    
    if verbose:
      print("[4/5] Creating new F...")
    sql_exec(query5)    
    sql_exec(query6)   
    if verbose:
      print("[5/5] Creating new R...") 
    sql_exec(query7)
    sql_exec(query8)
    sql_exec(query9d)
    
    add_constraints(newtab,verbose)
    return newtab
    
    
    
def union(T1,T2,target,verbose=True):
    (R1,C1,F1) = T1
    (R2,C2,F2) = T2
    r1att = ','.join(list(np.array(sql("SHOW columns FROM %s"%R1))[1:,0]))
    r2att = ','.join(list(np.array(sql("SHOW columns FROM %s"%R2))[1:,0]))
    if verbose:
        print("[*] Union (%s)"%target)
    assert(r1att == r2att)    
    newtab = tabname(target)
    drop_tables(newtab,verbose)

    query1 = 'CREATE TABLE new_id AS SELECT R1.id as id_1,1 as id_2 FROM %s as R1 UNION SELECT R2.id as id_1,2 as id_2 FROM %s as R2' % (R1,R2)
    query2a = 'ALTER TABLE new_id ADD COLUMN id INT PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST'
    query2b = 'ALTER TABLE new_id ADD KEY(id_1,id_2)'

    query9a = 'CREATE TABLE new_clusters AS SELECT F.cluster as id_1,1 as id_2 FROM %s as F UNION SELECT F.cluster as id_1,2 as id_2 FROM %s as F' % (F1,F2)
    query9b = 'ALTER TABLE new_clusters ADD COLUMN id INT PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST'
    query9c = 'ALTER TABLE new_clusters ADD KEY(id_1,id_2)'
    
    query3 = 'CREATE TABLE C_%s AS SELECT I.id as tuple_id,C.attribut,C.lwid,C.value FROM %s AS C JOIN new_id I ON I.id_1 = C.tuple_id AND I.id_2 = 1 UNION SELECT I.id as tuple_id,C.attribut,C.lwid,C.value FROM %s AS C JOIN new_id I ON I.id_1 = C.tuple_id AND I.id_2 = 2' %(target,C1,C2)
    
    query4 = 'CREATE TABLE F_%s AS SELECT I.id as tuple_id,F.attribut,C.id as cluster  FROM %s AS F JOIN new_id I JOIN new_clusters as C ON I.id_1 = F.tuple_id AND I.id_2 = 1 AND C.id_1 = F.cluster WHERE C.id_2 = 1 UNION SELECT I.id as tuple_id,F.attribut,C.id as cluster   FROM %s AS F JOIN new_id I JOIN new_clusters C ON I.id_1 = F.tuple_id AND I.id_2 = 2 AND C.id_1 = F.cluster WHERE C.id_2 = 2' %(target,F1,F2)
    
    query5 ='CREATE TABLE R_%s AS SELECT * FROM (SELECT I.id as id,%s FROM new_id as I JOIN %s as R1 ON I.id_1 = R1.id AND I.id_2 = 1 UNION SELECT I.id as id,%s FROM new_id as I JOIN %s as R2 ON I.id_1 = R2.id AND I.id_2 = 2) as T ORDER BY T.id' % (target,r1att,R1,r2att,R2)
    
    query6 ='DROP TABLE new_id'
    query9d = 'DROP TABLE new_clusters'
    
    if verbose:
        print("[1/5] Create new ids...")
    sql_exec(query1)    
    sql_exec(query2a)
    sql_exec(query2b)  
    if verbose:
        print("[2/5] Create new clusters...")  
    sql_exec(query9a)
    sql_exec(query9b)
    sql_exec(query9c)
    if verbose:
        print("[3/5] Creating new C...")  
    sql_exec(query3)    
    if verbose:
        print("[4/5] Creating new F...")
    sql_exec(query4)   
    if verbose:
        print("[5/5] Creating new R...") 
    sql_exec(query5)
    sql_exec(query6)
    sql_exec(query9d)
    
    add_constraints(newtab,verbose)
    return newtab
    
    
    

def selection_const(T,attribut,condition,target,verbose=True):
    (R,C,F) = T
    if verbose:
        print("[*] Selection %s %s (%s)"%(attribut,condition,target))
    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    
    query_R_1 = 'SELECT * FROM %s WHERE %s %s' %(R,attribut,condition)
    query_R_2 = 'SELECT R.* FROM %s as R JOIN %s as C  ON R.id = C.tuple_id WHERE R.%s is NULL and attribut = "%s" and value %s ' % (R,C,attribut,attribut,condition)
    
    query_R = 'CREATE TABLE R_%s AS SELECT * FROM  (%s UNION %s) as T ORDER BY id' %(target,query_R_1,query_R_2)
    
    if verbose:
      print("[1/4] Creating new R...")
    sql_exec(query_R)
    
    query_F = 'CREATE TABLE F_%s AS SELECT * FROM %s WHERE tuple_id IN (SELECT id FROM R_%s)' % (target,F,target)
    query_C = 'CREATE TABLE C_%s AS SELECT * FROM %s WHERE tuple_id IN (SELECT id FROM R_%s)' % (target,C,target)
    
    if verbose:
      print("[2/4] Creating new F...")
    sql_exec(query_F)
    
    if verbose:
      print("[3/4] Creating new C...")
    sql_exec(query_C)    
    add_constraints(newtab,verbose)
    
    replace_null = 'REPLACE INTO C_%s SELECT tuple_id,attribut,lwid,NULL as value FROM C_%s WHERE attribut = "%s" and NOT(value %s)' %(target,target,attribut,condition)
    if verbose:
      print("[4/4] Select tuples...")
    sql_exec(replace_null)
    
    weak_normalize(newtab,verbose)
    return newtab
    
    
def projection(T,attribut_list,target,merging='Classic',verbose=True):
    (R,C,F) = T
    if verbose:
        print("[*] Projection (%s)"%target)
        
    attribut_list = ['id'] + attribut_list
    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    
    query2 = 'CREATE TABLE attributs_%s ( name VARCHAR(255))'%target
    query3 ='INSERT INTO attributs_%s VALUES ("%s")'%(target,'"),("'.join(attribut_list))

    if verbose:
        print("[1/4] Creating attribute table...")
    sql_exec(query2)
    sql_exec(query3)
    
    
    query4a= 'SELECT %s FROM %s' %(','.join(attribut_list),R)    
    query4 = 'CREATE TABLE R_%s AS %s ' %(target,query4a)
    query5 = 'CREATE TABLE C_%s AS SELECT * FROM %s'%(target,C)
    query6 = 'CREATE TABLE F_%s AS SELECT * FROM %s'%(target,F)
    
    if verbose:
        print("[2/4] Creating new tables...")
    sql_exec(query4)
    sql_exec(query5)
    sql_exec(query6)
    
    add_constraints(newtab,verbose)
    
    # 1. Merge clusters with tid above
    #Faux
    
    #tid_with_null = 'SELECT tuple_id from %s WHERE value is NULL and attribut not in (SELECT * FROM attributs_%s)' % (C,target)    
    components_two_tuples = 'SELECT DISTINCT tuple_id FROM (SELECT cluster,count(distinct tuple_id) as n FROM %s as F GROUP BY cluster HAVING  n > 1) as T JOIN %s as F ON T.cluster = F.cluster' % (F,F)
    components_one_tuple = 'SELECT DISTINCT C.tuple_id FROM %s as F JOIN %s as C ON C.attribut = F.attribut AND C.tuple_id = F.tuple_id WHERE C.tuple_id NOT IN (%s) AND C.attribut not in (SELECT *  FROM attributs_%s) and C.value is NULL' % (F,C,components_two_tuples,target)
    
    update_in_C = 'INSERT INTO C_%s SELECT C.tuple_id,C.attribut,max(lwid)+1,NULL FROM (%s) as P JOIN %s as C ON P.tuple_id = C.tuple_id WHERE C.attribut IN (SELECT * FROM attributs_%s) AND (C.tuple_id,C.attribut) NOT IN (SELECT tuple_id,attribut FROM %s WHERE value is NULL) GROUP BY C.tuple_id,C.attribut' % (target,components_one_tuple,C,target,C)


    if verbose:
        print("[2.5/4] Mergeing clusters...")
    sql_exec(update_in_C)
    # + clusters with other tuples
    (newR,newC,newF) = newtab
    
    #query_rec = 'SELECT F1.cluster as c1, F2.cluster as c2 FROM %s AS F1 JOIN %s as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.cluster < F2.cluster AND F1.tuple_id in (%s)' % (newF,newF,tid_with_null)
    query_rec = 'SELECT DISTINCT F1.cluster as c1, F2.cluster as c2 FROM %s AS F1 JOIN %s as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.cluster < F2.cluster AND F1.tuple_id in (%s)' % (newF,newF,components_two_tuples)
    if verbose:
        print("[3/4] Mergeing clusters...")
    if merging == 'Classic':
        merge_clusters(newtab,query_rec,verbose)
    elif merging == '1by1':
        merge_clusters_one_by_one(newtab,query_rec,verbose)
    
    # 2. Propagate null values 
    
    propagate_null(newtab,verbose)
    
    # 3. Keep only good attributes
    
    query7 = 'DELETE FROM %s WHERE attribut not in (SELECT * FROM attributs_%s)' % (newC,target)
    query8 = 'DELETE FROM %s WHERE attribut not in (SELECT * FROM attributs_%s)' % (newF,target)
    query9 = "DROP TABLE attributs_%s"%target
    
    if verbose:
        print("[4/4] Projecting...")
    sql_exec(query7)
    sql_exec(query8)
    sql_exec(query9)

    return newtab
    
    
    
def selection(T,attribut1,attribut2,operateur,target,merging="Classic",int=False,verbose=True):
    (R,C,F) = T
    if verbose:
        print("[*] Selection %s %s %s (%s)"%(attribut1,operateur,attribut2,target))
    
    newtab = tabname(target)
    drop_tables(newtab,verbose)
    if int:
        mul = "1*"
    else:
        mul  = ""
    
    select1 = 'SELECT * FROM %s WHERE %s %s %s' % (R,attribut1,operateur,attribut2)
    select2 = 'SELECT R.* FROM %s as R JOIN %s as C ON R.id = C.tuple_id WHERE (C.attribut = "%s" AND value %s R.%s) OR (C.attribut="%s" AND R.%s %s value)' % (R,C,attribut1,operateur,attribut2,attribut2,attribut1,operateur)
    #select3 = 'SELECT DISTINCT R.* FROM %s as R JOIN %s as C1 JOIN %s as C2 ON R.id = C1.tuple_id AND R.id = C2.tuple_id WHERE C1.attribut = "%s" AND C2.attribut = "%s" AND C1.value %s C2.value'  % (R,C,C,attribut1,attribut2,operateur)
    select3 = 'SELECT DISTINCT R.* FROM %s as R JOIN %s as C1 JOIN %s as C2 JOIN %s as F1 JOIN %s as F2 ON C1.attribut = F1.attribut AND C1.tuple_id = F1.tuple_id AND C2.attribut = F2.attribut AND C2.tuple_id = F2.tuple_id AND R.id = C1.tuple_id AND R.id = C2.tuple_id WHERE C1.attribut = "%s" AND C2.attribut = "%s" AND %sC1.value %s %sC2.value AND ((F1.cluster = F2.cluster AND C1.lwid = C2.lwid) OR F1.cluster <> F2.cluster)'% (R,C,C,F,F,attribut1,attribut2,mul,operateur,mul)
    selectUnion = 'SELECT * FROM (%s UNION %s UNION %s) as T ORDER BY id' % (select1,select2,select3)
    query1 = 'CREATE TABLE R_%s AS %s' % (target,selectUnion)
    query2 = 'CREATE TABLE C_%s AS SELECT * FROM %s WHERE tuple_id IN (SELECT id FROM R_%s)'%(target,C,target)
    query3 = 'CREATE TABLE F_%s AS SELECT * FROM %s WHERE tuple_id IN (SELECT id FROM R_%s)'%(target,F,target)
    # different clusters
    if verbose:
        print("[1/3] Creating new tables...")
    sql_exec(query1)
    sql_exec(query2)
    sql_exec(query3)
    
    add_constraints(newtab,verbose)
    
    (newR,newC,newF) = newtab
    
    query18 = 'REPLACE INTO %s SELECT C.tuple_id, C.attribut, C.lwid, NULL FROM %s as C JOIN %s as R ON R.id = C.tuple_id WHERE C.attribut = "%s" AND NOT(R.%s %s C.value)' % (newC,newC,newR,attribut2,attribut1,operateur)
    query19 = 'REPLACE INTO %s SELECT C.tuple_id, C.attribut, C.lwid, NULL FROM %s as C JOIN %s as R ON R.id = C.tuple_id WHERE C.attribut = "%s" AND  NOT(C.value %s R.%s) '  % (newC,newC,newR,attribut1,operateur,attribut2)
    if verbose:
        print("[1/3] Selecting tuples...")
    sql_exec(query18)
    sql_exec(query19)
        
    query_rec = 'SELECT DISTINCT c1,c2 FROM (SELECT F1.tuple_id,F1.cluster as c1, F2.cluster as c2 FROM %s as F1 JOIN %s as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.attribut = "%s" AND F2.attribut = "%s" AND F1.cluster < F2.cluster UNION SELECT F1.tuple_id,F2.cluster as c1, F1.cluster as c2 FROM %s as F1 JOIN %s as F2 ON F1.tuple_id = F2.tuple_id WHERE F1.attribut = "%s" AND F2.attribut = "%s" AND F2.cluster < F1.cluster) as T WHERE tuple_id IN (SELECT id FROM %s)' % (newF,newF,attribut1,attribut2,newF,newF,attribut1,attribut2,newR)
        
        
    if verbose:
        print("[3/3] Mergeing clusters...")
    
    if merging == "Classic":
        merge_clusters_selection(newtab,query_rec,attribut1,attribut2,operateur,verbose)
    elif merging == "1by1":
        merge_clusters_one_by_one(newtab,query_rec,verbose)
        
        query4a = 'SELECT C1.tuple_id,C1.lwid FROM %s as C1 JOIN %s as C2 ON C1.tuple_id = C2.tuple_id AND C1.lwid = C2.lwid WHERE C1.attribut = "%s" AND C2.attribut = "%s" AND not(C1.value %s C2.value) ' % (newC,newC,attribut1,attribut2,operateur)
        query4 = 'REPLACE INTO %s SELECT C.tuple_id,C.attribut,C.lwid,NULL FROM %s as C JOIN (%s) as T ON C.tuple_id = T.tuple_id AND C.lwid = T.lwid WHERE C.attribut = "%s" OR C.attribut = "%s"' % (newC,newC,query4a,attribut1,attribut2)
        if verbose:
            print("[3/3] Selecting tuples...")
        sql_exec(query4)
        weak_normalize(newtab,verbose)

    
    return newtab
    
def difference(T1,T2,target):
    ()
    
    
