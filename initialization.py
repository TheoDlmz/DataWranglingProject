import numpy as np
import time
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=PASSWORD,
  database=DATABASE
)

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
# This function create a classic database containing all trees entries
def populate_db(a,verbose=True):
    if verbose:
        print("Deleting old tuples...")
    sql_exec("DELETE FROM arbres")
    id = 0
    insert = "INSERT INTO arbres (id,type_emplacement,domanialite,arrondissement,adrr,id_empl,name,genre,espece,variete,circonf,hauteur,developpement,remarquable) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    records = []
    if verbose:
        print("Inserting new tuples...")
    for i in range(1,len(a)):
        type_emplacement = a[i,1]
        domanialite = a[i,2]
        arrondissement = a[i,3]
        addr = a[i,6]
        id_empl= a[i,7]
        name = a[i,8]
        genre = a[i,9]
        espece = a[i,10]
        variete = a[i,11]
        circonf = int(float(a[i,12]))
        hauteur = int(float(a[i,13]))
        developpement = a[i,14]
        if a[i,15] == "":
            remarquable = "0"
        else:
            remarquable = int(a[i,15])
        record_i = [id,type_emplacement,domanialite,arrondissement,addr,id_empl,name,genre,espece,variete,circonf,hauteur,developpement,remarquable]
        records.append(record_i)
        id += 1
        if i%10000 == 9999:
            if verbose:
                print(i+1,"/",len(a))
            mycursor = mydb.cursor()
            mycursor.executemany(insert, records)
            mydb.commit()
            records = []
    mycursor = mydb.cursor()
    mycursor.executemany(insert, records)
    mydb.commit()
    
    
    ##
    
    
    #This function create an uncertain database
    
def populate_db_incomplete(a,verbose=True):
    if verbose:
        print("Deleting old Tuple...")
    sql_exec("DELETE FROM arbres_0;")
    sql_exec("DELETE FROM C;")
    sql_exec("DELETE FROM F;")
    mydb.commit()
    print("Deleted")
    insert_arbres = "INSERT INTO arbres_0 (id,type_emplacement,domanialite,arrondissement,adrr,id_empl,name,genre,espece,variete,circonf,hauteur,developpement,remarquable) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    insert_C = "INSERT INTO C VALUES (%s,%s,%s,%s);"
    insert_F = "INSERT INTO F VALUES (%s,%s,%s);"
    records = []
    clusters = []
    members = []
    cluster_i = 0
    pssword = [0,0,0,0,0,0]
    totalf,totalc,totalr = 0,0,0
    id = 0
    if verbose:
        print("Inserting new tuples...")
        
    for i in range(1,len(a)):
        type_emplacement = a[i,1]
        domanialite = a[i,2]
        arrondissement = a[i,3]
        addr = a[i,6]
        id_empl= a[i,7]
        name = a[i,8]
        genre = a[i,9]
        espece = a[i,10]
        variete = a[i,11]
        circonf = int(float(a[i,12]))
        hauteur = int(float(a[i,13]))
        developpement = a[i,14]
        if a[i,15] == "":
            remarquable = 0
        else:
            remarquable = int(a[i,15])
            
            
        # Possible worlds : If stage of devlopment is not specified, add 4 possible world, one for each possible stage
        if developpement == "":
            for j,dev in enumerate(["J","JA","A","M"]):
                clusters.append([id,"developpement",j,dev])
            members.append([id,"developpement",cluster_i])
            cluster_i += 1
            pssword[0] += np.log(4)
            developpement = None
            
        # Possible worlds : If height > 45m, try substring of 1 or 2 digits and try to replace first digit with 1
        if hauteur > 45:
            j = 0
            str_h = str(hauteur)
            if hauteur < 100:
                clusters.append([id,"hauteur",0,10+int(str_h[1])])
                j += 1
            seen = []
            for j_2 in range(len(str_h)):
                h = int(str_h[j_2])
                if h not in seen and h > 0:
                    seen.append(h)
                    clusters.append([id,"hauteur",j,h])
                    j += 1
            for j_2 in range(len(str_h)-1):
                h = int(str_h[j_2:j_2+2])
                if h not in seen and h < 30 and h > 0:
                    seen.append(h)
                    clusters.append([id,"hauteur",j,h])
                    j += 1

            pssword[1]  += np.log(j)
            members.append([id,"hauteur",cluster_i])
            cluster_i += 1
            hauteur = None
        
        # Possible worlds : If 45m >= height > 30m, the tree is either noticeable, or the height is not the real one
        elif hauteur > 30 and remarquable == 0:
            # Remarquable = 1
            clusters.append([id,"hauteur",0,hauteur])
            clusters.append([id,"remarquable",0,1])
            
            str_h = str(hauteur)
            clusters.append([id,"hauteur",1,int(str_h[0])])
            clusters.append([id,"hauteur",2,10+int(str_h[1])])
            clusters.append([id,"hauteur",3,20+int(str_h[1])])
            for j in range(1,4):
                clusters.append([id,"remarquable",j,0])
            j = 4

            pssword[2] += np.log(4)
            members.append([id,"hauteur",cluster_i])
            members.append([id,"remarquable",cluster_i])
            cluster_i += 1

            hauteur = None
            remarquable = None
        
        # Possible worlds : If circumference > 6m95 (not possible), try substrings
        if circonf > 695:
            str_c = str(circonf)
            seen = []
            j =0
            for j_2 in range(len(str_c)-1):
                c = int(str_c[j_2:j_2+2])
                if c not in seen and c > 10:
                    seen.append(c)
                    clusters.append([id,"circonf",j,c])
                    j += 1
                    
            for j_2 in range(len(str_c)-2):
                c = int(str_c[j_2:j_2+3])
                if c not in seen and c < 695 and c >= 10:
                    seen.append(c)
                    clusters.append([id,"circonf",j,c])
                    j += 1

            pssword[3] += np.log(j)
            members.append([id,"circonf",cluster_i])
            cluster_i += 1
            circonf = None
        
        # Possible worlds : If circumference = 0cm (not possible), draw 3 random values with a gaussian
        if circonf == 0:
            seen = []
            j =0
            while j <= 2:
                c = int(np.random.normal(91,59))
                if c >= 10 and c not in seen:
                    clusters.append([id,"circonf",j,c])
                    seen.append(c)
                    j += 1

            pssword[4] += np.log(j)
            members.append([id,"circonf",cluster_i])
            cluster_i += 1
            circonf = None
        
        # Possible worlds : If height= 0cm (not possible), draw 3 random values with a gaussian
        if hauteur == 0:
            seen = []
            j =0
            while j <= 2:
                h = int(np.random.normal(10.2,5.2))
                if h > 0 and h not in seen:
                    clusters.append([id,"hauteur",j,h])
                    seen.append(h)
                    j += 1

            pssword[5] += np.log(j)
            members.append([id,"hauteur",cluster_i])
            cluster_i += 1
            hauteur = None
        record_i = [id,type_emplacement,domanialite,arrondissement,addr,id_empl,name,genre,espece,variete,circonf,hauteur,developpement,remarquable]
        records.append(record_i)
        id += 1
        if i%10000 == 9999:
            if verbose:
                print(i+1,"/",len(a))
                totalr += len(records)
                totalc += len(clusters)
                totalf += len(members)
            mycursor = mydb.cursor()
            mycursor.executemany(insert_arbres, records)
            mycursor.executemany(insert_C, clusters)
            mycursor.executemany(insert_F, members)
            mydb.commit()   
            records = []
            clusters = []
            members = []
    mycursor = mydb.cursor()
    mycursor.executemany(insert_arbres, records)
    mycursor.executemany(insert_C, clusters)
    mycursor.executemany(insert_F, members)
    mydb.commit()
    if verbose:
        print("records : ",totalr)
        print("clusters : ",totalc)
        print("members :",totalf)
        print("possible worlds :",pssword)
    
    return ("arbres_0","C","F")
        
##


def chase(T,attribut_contr,min_value,attribut_target,value,verbose=True):
    (R,C,F) = T
    # attribut_contr > min_value ===> attribut_target <> value
    if verbose:
        print("[*] Chase...")

    
    # PARTIE 1 : ATTRIBUT 1 ET 2 SONT NULL
    query1 = 'SELECT distinct tuple_id from %s as C where attribut="%s" AND value > %s AND tuple_id IN (SELECT distinct tuple_id FROM %s as C where attribut="%s" and value = "%s")' % (C,attribut_contr,min_value,C,attribut_target,value)
    # Les tuples concernés par la contrainte
    
    query_rec = 'SELECT DISTINCT c1,c2 FROM (SELECT F1.tuple_id, F1.cluster as c1, F2.cluster as c2 FROM %s as F1 JOIN %s as F2 ON F2.tuple_id = F1.tuple_id WHERE F1.attribut = "%s" AND F2.attribut = "%s" AND F1.cluster < F2.cluster UNION SELECT F1.tuple_id, F2.cluster as c1, F1.cluster as c2 FROM %s as F1 JOIN %s as F2 ON F2.tuple_id = F1.tuple_id WHERE F1.attribut = "%s" AND F2.attribut = "%s" AND F2.cluster < F1.cluster) as T WHERE tuple_id IN (%s)' % (F,F,attribut_contr,attribut_target,F,F,attribut_contr,attribut_target,query1)
    
    if verbose:
        print("[1/4] Mergeing clusters...")
    merge_clusters(T,query_rec,verbose)
    
    
    # Supprimer les mauvais tuples :
    case1 = 'SELECT tuple_id,lwid FROM C WHERE attribut="%s" AND value > %s' % (attribut_contr,min_value)
    case2 = 'SELECT tuple_id,lwid FROM C WHERE attribut ="%s" AND value = "%s"' %(attribut_target,value)
    # Ci dessous peut supprimer des bons trucs : regarder le cluster -> Replace by "NULL"
    
    query2 = 'REPLACE INTO C SELECT tuple_id,attribut,lwid,NULL FROM C WHERE (tuple_id,lwid) in (SELECT DISTINCT T1.tuple_id,T1.lwid FROM (%s) as T1 JOIN (%s) as T2 on T2.tuple_id = T1.tuple_id and T1.lwid = T2.lwid) AND (attribut = "%s" OR attribut="%s")' % (case1,case2,attribut_contr,attribut_target)
    #query2 = 'DELETE FROM C WHERE (tuple_id,lwid) in (SELECT DISTINCT T1.tuple_id,T1.lwid FROM (%s) as T1 JOIN (%s) as T2 on T2.tuple_id = T1.tuple_id and T1.lwid = T2.lwid) AND (attribut = "%s" OR attribut="%s")' % (case1,case2,attribut_contr,attribut_target)
    if verbose:
        print("[2/4] Deleting tuples in C...")
    sql_exec(query2)
    
    
    # PARTIE 2 : conditions 2 dans R0
    query3a = 'SELECT distinct tuple_id,lwid from C where attribut="%s" and value > %s and tuple_id in (SELECT id FROM arbres_0 where %s = "%s")' % (attribut_contr,min_value,attribut_target,value)
    
    query3 = 'REPLACE INTO C SELECT tuple_id,attribut,lwid,NULL FROM C WHERE (tuple_id,lwid) in (SELECT tuple_id,lwid FROM (%s) as T) AND attribut = "%s"'% (query3a,attribut_contr)
    #query3 = 'DELETE FROM C WHERE (tuple_id,lwid) in (SELECT tuple_id,lwid FROM (%s) as T) AND attribut = "%s"'% (query3a,attribut_contr)
    if verbose:
        print("[3/4] Deleting tuples in C...")
    sql_exec(query3)
    
    
    # PARTIE 3 : conditions 1 dans R0
    query4a = 'SELECT distinct tuple_id,lwid from C where attribut = "%s" and value = "%s" and tuple_id in (SELECT id FROM arbres_0 where %s > %s)' % (attribut_target,value,attribut_contr,min_value)

    query4 = 'REPLACE INTO C SELECT tuple_id,attribut,lwid,NULL FROM C WHERE (tuple_id,lwid) in (SELECT tuple_id,lwid FROM (%s) as T) AND attribut = "%s"'% (query4a,attribut_target)
    #query4 = 'DELETE FROM C WHERE (tuple_id,lwid) in (SELECT tuple_id,lwid FROM (%s) as T) AND attribut = "%s"'% (query4a,attribut_target)
    print("[4/4] Deleting tuples in C ...")
    sql_exec(query4)
    
    query5 = 'DELETE FROM C WHERE value is NULL'
    sql_exec(query5)
    
    
    T = ("arbres_0","C","F")
    #weak_normalize(T,verbose)

    
    
## INITIALISE DB


def restart(verbose=True):
    a = np.genfromtxt('data/les-arbres.csv', delimiter=';',dtype=None)

    t0 = time.time()
    populate_db(a,verbose)
    t1 = time.time()
    R0 = populate_db_incomplete(a,verbose)
    t2 = time.time()
    
    chase(R0,"hauteur","25","developpement","JA",verbose)
    t3 = time.time()
    chase(R0,"hauteur","20","developpement","J",verbose)
    t4 = time.time()
    chase(R0,"circonf","250","developpement","JA",verbose)
    t5 = time.time()
    chase(R0,"circonf","200","developpement","J",verbose)
    t6 = time.time()
    if verbose:
        print("[*] Time :")
        print("[0/5] Building db basic : %f" % (t1-t0))
        print("[1/5] Building db : %f" %(t2-t1))
        print("[2/5] Chase 1 : %f"%(t3-t2))
        print("[3/5] Chase 2 : %f"%(t4-t3))
        print("[4/5] Chase 3 : %f"%(t5-t4))
        print("[5/5] Chase 4 : %f"%(t6-t5))
    return ("arbres_0","C","F")