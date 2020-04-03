import numpy as np
import time
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector



a = np.genfromtxt('data/les-arbres.csv', delimiter=';',dtype=None)


# This function plot every (circumference, height) for a given tree and use 1 color for each stage of development
def plot_evol(esp="Tilleul"):
    tabc = ["#b7f01d","#e8ce38","#e07912","#f74307"]
    X = [[],[],[],[]]
    Y = [[],[],[],[]]
    for arbre in a[1:]:
        if arbre[8] == esp:
            x = float(arbre[12])
            y = float(arbre[13])
            d = arbre[14]
            if x >0 and x < 500 and y>0 and y < 30:
                if d == 'J':
                    X[0].append(x)
                    Y[0].append(y)
                elif d == 'JA':
                    X[1].append(x+2.5)
                    Y[1].append(y)
                elif d == 'A':
                    X[2].append(x)
                    Y[2].append(y+2.5)
                elif d == 'M':
                    X[3].append(x+2.5)
                    Y[3].append(y+2.5)
    for i in range(4):
        plt.plot(X[i],Y[i],'o',color=tabc[i],markersize=2.5,alpha=1)
    plt.show()


# This function plot an histogram of heights for each stage of development
def plot_arbres(a):
    dict_hauteur = dict()
    for arbre in a[1:]:
        dev = arbre[14]
        hauteur = int(float(arbre[13]))
        if arbre[15] == "":
            remarquable = "0"
        else:
            remarquable = int(arbre[15])
        if dev == "" or hauteur > 45 or hauteur== 0:
            continue
        if dev not in dict_hauteur.keys():
            dict_hauteur[dev] = [hauteur]
        else:
            dict_hauteur[dev].append(hauteur)
    
    for dev in dict_hauteur.keys():
        plt.hist(dict_hauteur[dev],20,label=dev,alpha=0.5,density=True)
    plt.legend()
    plt.show()


# This function plot an histogram of heights for one particular tree specie
def plot_genre(a,genre_ok="Tilleul"):
    dict_hauteur = []
    for arbre in a[1:]:
        genre = arbre[8]
        hauteur = int(float(arbre[13]))
        if hauteur > 45 or hauteur== 0 or genre != genre_ok:
            continue
        dict_hauteur.append(hauteur)
    print(len(dict_hauteur))
    plt.hist(dict_hauteur,20,alpha=0.5,density=True)
    plt.show()


