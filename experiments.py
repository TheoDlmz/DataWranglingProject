import numpy as np
import time
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=PASSWD,
  database=DATABASE
)


##
print("QUERY 1")
#Selection with const, Projection
print("What are the specie, height (m) ,circumference (cm),public state of mature trees from the 5th borough of Paris?")

sql_exec("DROP VIEW IF EXISTS q1")
t0 = time.time()
sql_exec("CREATE VIEW q1 AS SELECT name,hauteur,circonf,domanialite FROM arbres WHERE arrondissement = 'PARIS 5E ARRDT' AND developpement='M'")
sql("SELECT name,hauteur,circonf,domanialite FROM arbres WHERE arrondissement = 'PARIS 5E ARRDT' AND developpement='M'")
t1 = time.time()

verbose=False
t2 = time.time()
q1 = projection(selection_const(selection_const(R0,"arrondissement",'= "PARIS 5E ARRDT"',"qtemp1",verbose=verbose),"developpement",'= "M"',"qtemp2",verbose=verbose),["name","hauteur","circonf","domanialite"],"q1",verbose=verbose)
t3 = time.time()
strong_normalize(q1,verbose)
t4 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s (+ % s for normalization)"% (t3-t2,t4-t3))
##

print("QUERY 2")
#Selection const, projection, operation
print("What are the specie, height (cm),circumference (cm),stage of development of trees of the wood around Paris (Bois de boulogne, Bois de Vincennes) larger than 250 cm of circumference ?")

sql_exec("DROP VIEW IF EXISTS q2")
t0 = time.time()
sql_exec("CREATE VIEW q2 AS SELECT name,hauteur*100 as hauteur_cm,circonf,developpement FROM arbres WHERE arrondissement LIKE 'BOIS%' AND circonf > 250")

sql("SELECT name,hauteur*100 as hauteur_cm,circonf,developpement FROM arbres WHERE arrondissement LIKE 'BOIS%' AND circonf > 200")
t1 = time.time()

verbose=False
t2 = time.time()
q2 = projection(operation_one(selection_const(selection_const(R0,"arrondissement",'LIKE "BOIS%"',"qtemp1",verbose=verbose),"circonf",'> 250',"qtemp2",verbose=verbose),"hauteur","%s*100","hauteur_cm","qtemp1",verbose=verbose),["name","hauteur_cm","circonf","developpement"],"q2",verbose=verbose)
t3 = time.time()
strong_normalize(q2,verbose)
t4 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s (+ % s for normalization)"% (t3-t2,t4-t3))

##

print("QUERY 3")
print("What are the species, borough, height (m), circumference (cm), importance of trees such that their circumference in cm x5 is greater than their height in cm?")
#Selection, projection, operation

sql_exec("DROP VIEW IF EXISTS q3")
t0 = time.time()
sql_exec("CREATE VIEW q3 AS SELECT name,arrondissement, hauteur, circonf,remarquable FROM arbres WHERE circonf*1 > hauteur*100")
sql("SELECT name,arrondissement, hauteur, circonf,remarquable FROM arbres WHERE circonf*1 > hauteur*100")
t1 = time.time()


verbose=False
t2 = time.time()
q3 = projection(selection(operation_one(R0,"hauteur","%s*20","hauteur_20","qtemp1",verbose=verbose),"circonf","hauteur_20",">","qtemp2",merging="Classic",int=True,verbose=verbose),["name","arrondissement","hauteur","circonf","remarquable"],"q3",verbose=verbose)
t3 = time.time()
strong_normalize(q3,True)
t4 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s (+ % s for normalization)"% (t3-t2,t4-t3))
##


print("QUERY 3 (v2)")
print("What are the species, borough, height (m), circumference (cm), importance of trees such that their circumference in cm x5 is greater than their height in cm?")
#Selection, projection, operation

t0 = time.time()
sql("SELECT name,arrondissement, hauteur, circonf,remarquable FROM arbres WHERE circonf*5 > hauteur*100")
t1 = time.time()


verbose=False

#q3 = projection(selection(operation_one(R0,"hauteur","%s*20","hauteur_20","qtemp1",verbose=verbose),"circonf","hauteur_20",">","qtemp2","1by1",int=True,verbose=verbose),["name,arrondissement,hauteur,circonf,remarquable"],"q3",verbose=verbose)
t3 = time.time()
strong_normalize(q3,verbose)
t4 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s (+ % s for normalization)"% (t3-t2,t4-t3))

##


print("QUERY 4")
print("What is the average height of trees in Q2 ?")
# Aggregation

verbose=False
t0 = time.time()
sql("SELECT avg(hauteur_cm) FROM q2")
t1 = time.time()
avg(q2,"hauteur_cm","q2",verbose=verbose)
t2 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s"% (t2-t1))


##

print("QUERY 5 ")
print("Species and circumference in Q1 or Q2?")
#Union
verbose= False
sql_exec("DROP VIEW IF EXISTS q5")
t0 = time.time()
sql_exec("CREATE VIEW q5 AS SELECT name,circonf FROM q1 UNION ALL SELECT name,circonf FROM q2")
sql("SELECT name,circonf FROM q1 UNION SELECT name,circonf FROM q2")
t1 = time.time()
union(projection(q1,["name","circonf"],"qtemp1",verbose=verbose),projection(q2,["name","circonf"],"qtemp2",verbose=verbose),"q5",verbose=verbose)
t2 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s"% (t2-t1))


##

print("QUERY 6")
print("Tree of Q1 such that one tree of Q2 has the same height but is not of the same specie. What are their species ?")
# Rename, Cross Product, Selection,

verbose=False
sql_exec("DROP VIEW IF EXISTS q6")
t0 = time.time()
sql_exec("CREATE VIEW q6 AS SELECT q1.name as name1,q2.name as name2,q1.circonf,q1.hauteur,q2.hauteur_cm,q2.developpement FROM q1 JOIN q2 ON q1.name <> q2.name AND q1.circonf = q2.circonf")
sql("SELECT q1.name as name1,q2.name as name2,q1.circonf,q1.hauteur,q2.hauteur_cm,q2.developpement FROM q1 JOIN q2 ON q1.name <> q2.name AND q1.circonf = q2.circonf")
t1 = time.time()


t2 = time.time()
q6 = projection(selection(selection(cross_product(q1,rename(q2,[("name","name_bis"),("circonf","circonf_bis")],"qtemp1",verbose=verbose),"qtemp2",verbose=verbose),"name","name_bis","<>","qtemp1",verbose=verbose),"circonf","circonf_bis","=","qtemp2",int=True,verbose=verbose),["name","name_bis","circonf","developpement","hauteur","hauteur_cm"],"q6",verbose=verbose)
t3 = time.time()
strong_normalize(q6,True)
t4 = time.time()
print("Classic db : %s s"%(t1-t0))
print("Uncertain db : %s s (+ % s for normalization)"% (t3-t2,t4-t3))
##

