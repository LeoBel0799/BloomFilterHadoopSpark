import sys

from pyspark import SparkContext
from operator import add
import numpy as np
import mmh3
import math


#round like java
def roundRating(val):
    if val % 1 == 0.5:
        val = int(val) + 1
    else:
        val = round(val)
    return val

#do the hash depending also the number of k value (number of hash function for the bloom)
def hash(filters, rating, movie_id):  # filtri, return della funzione , item successivo
    k = int(-(np.log(float(pvalue)) / np.log(2)))
    movie_id = movie_id.encode('utf-8')
    size = list_m[rating - 1]

    for i in range(0, k):
        position = mmh3.hash(movie_id, 50 * i) % size
        if position < 0:
            position = abs(position)
            filters[rating][position] = 1
        else:
            filters[rating][position] = 1


# calcolo m + concatenazione per avere rating m and counting
def fillM(alist, pvalue):
    tlist = list(zip(*alist))
    m = []
    listCount = tuple([int(x) for x in tlist[1]])
    tlist2 = list(listCount)
    for index in range(len(tlist2)):
        m.append(int((-(tlist2[index]) * np.log(float(pvalue)) / (pow(np.log(2), 2))) + 1))
    listm = list(m)
    return listm


def orFilter(filtro1, filtro2):
    count = -1

    for i in filtro2:
        count = count + 1
        if i == 1:
            filtro1[count] = 1
    return filtro1


def initFilter():
    f1 = []
    for i in range(list_m.__getitem__(0)):
        f1.append(0)
    f2 = []
    for i in range(list_m.__getitem__(1)):
        f2.append(0)
    f3 = []
    for i in range(list_m.__getitem__(2)):
        f3.append(0)
    f4 = []
    for i in range(list_m.__getitem__(3)):
        f4.append(0)
    f5 = []
    for i in range(list_m.__getitem__(4)):
        f5.append(0)
    f6 = []
    for i in range(list_m.__getitem__(5)):
        f6.append(0)
    f7 = []
    for i in range(list_m.__getitem__(6)):
        f7.append(0)
    f8 = []
    for i in range(list_m.__getitem__(7)):
        f8.append(0)
    f9 = []
    for i in range(list_m.__getitem__(8)):
        f9.append(0)
    f10 = []
    for i in range(list_m.__getitem__(9)):
        f10.append(0)
    filters = {1: f1, 2: f2, 3: f3, 4: f4, 5: f5, 6: f6, 7: f7, 8: f8, 9: f9, 10: f10}
    return filters


def mapper(keyValueRDD):
    filters = initFilter()
    for i in keyValueRDD:
        hash(filters, i[0], i[1])
    return filters.items()

#arriva in ingresso array(filtro da controllare) ,  title , m(dimensione m)
def isMember(title,array,m):
    pvalue=sys.argv[3]
    k= int(-(np.log(float(pvalue)) / np.log(2)))
    for i in range(k):
        position = mmh3.hash(title, 50 * i) % m
        position = abs(position)
        if(int(array[position])!=1):
            return 0
    return 1



if __name__ == "__main__":

    # # parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: Cloud-Computing project <input file> <output file> <pvalue>", file=sys.stderr)
        sys.exit(-1)
    inputFile = sys.argv[1]
    pvalue = sys.argv[3]
    # connect to Hadoop Cluster
    master = "yarn"
    sc = SparkContext(master, "Cloud-Computing project")

    # create input file RDD
    inputRDD = sc.textFile(inputFile, 4)

    # Creo RDD chiave- valore con rating approsimato
    keyValueRDD = inputRDD.map(lambda x: (roundRating(float(x.split("\t")[1])), x.split("\t")[0]),
                               preservesPartitioning=True)

    # # Salva chiave: Rating e item per ogni voto
    valuesM = keyValueRDD.distinct().keys().map(lambda x: (x, 1)).reduceByKey(add).sortByKey()
    # # tuple con rating,count and m
    list_m = fillM(valuesM.collect(), float(pvalue))

    # Fase MAPP
    finalRDD = keyValueRDD.mapPartitions(mapper, preservesPartitioning=True).reduceByKey(lambda x, y: orFilter(x, y),
                                                                                         numPartitions=1).saveAsTextFile(
        sys.argv[2])
    sc.stop()

    #test begin



    print("________BEGIN TEST_______")
    import pandas as pd
    import numpy as np
    import re

    # connect to Hadoop Cluster
    master = "yarn"
    sc = SparkContext(master, "Cloud-Computing project")
    rdd = sc.textFile("/cloudproject/Spark/part-00000")
    llist = rdd.collect()
    arr = []
    i=0
    # "creation of bloom" from work done by spark --> ora i bloom sono negli array
    for line in llist:
        tot = line.split('[')
        # print(tot[2])
        tot = tot[1].split(']')
        # print(tot[0])
        tmp = tot[0].split(',')
        arr.append(tmp)
        #print(len(arr[i]))
        i = i + 1


    FP = [0] * 10
    TN = [0] * 10
    N = [0] * 10
    totale = 0
    rdd2 = sc.textFile("/cloudproject/data.tsv")
    inputRdd = rdd2.collect()
    for line in inputRdd:
        tot=line.split('\t')
        title = tot[0]
        rating=roundRating(float(tot[1]))
        N[rating - 1] = N[rating - 1] + 1
        totale = totale + 1
        for i in range(10):
            if (rating != i+1):
                if (isMember(title, arr[i], len(arr[i]) - 1) == 1):
                    # print(rating)
                    FP[i] = FP[i] + 1
                else:
                    TN[i] = TN[i] + 1
    print("ris = ")

    for m in range(10):
        print(FP[m] / ((totale - N[m])))










    #df = rdd.map(lambda x: x.split("\t")).toDF()








