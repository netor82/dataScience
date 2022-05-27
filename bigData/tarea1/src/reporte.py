from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf, sum, desc, row_number, avg
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
from pyspark.sql.window import Window

def main(n):
    ciclistas, rutas, actividades = cargarDatos()
    topProvincia, topKm = getTopNReports(n, ciclistas, rutas, actividades)

    # "Top n por provincia:"
    topProvincia.show()

    #"Top n con mejor promedio diario de km recorridos"
    topKm.show()

def cargarDatos():
    spark = SparkSession.builder.appName("Read Transactions").getOrCreate()

    ciclista_scheme = StructType([StructField('ciclista_id', StringType()),
                            StructField('nombre', StringType()),
                            StructField('provincia', StringType()),
                            ])

    ruta_scheme = StructType([StructField('ruta_id', IntegerType()),
                            StructField('ruta_nombre', StringType()),
                            StructField('longitud', IntegerType()),
                            ])

    actividad_scheme = StructType([StructField('ruta', IntegerType()),
                            StructField('ciclista', StringType()),
                            StructField('fecha', DateType()),
                            ])

    ciclistas = spark.read.csv("/data/ciclista.csv", schema=ciclista_scheme, header=False)
    rutas = spark.read.csv("/data/ruta.csv", schema=ruta_scheme, header=False)
    actividades = spark.read.csv("/data/actividad.csv", schema=actividad_scheme, header=False)

    return ciclistas, rutas, actividades


def getTopNReports(n, ciclistas, rutas, actividades):

    joint = joinThem(ciclistas, rutas, actividades)

    agregatedProvincia = getAgregatedProvincia(joint)
    agregatedKmDay = getAgregatedKmDay(joint)

    topProvincia = topNParticionado(n, agregatedProvincia, columnPartition="provincia", orderedBy="total_km")
    topKm = topN(n, agregatedKmDay, orderedBy="promedio_diario")

    return topProvincia, topKm


def joinThem(ciclistas, rutas, actividades):
    joint = ciclistas \
        .join(actividades, ciclistas.ciclista_id == actividades.ciclista, "leftouter") \
        .join(rutas, actividades.ruta == rutas.ruta_id, "leftouter")
    
    return joint


def getAgregatedProvincia(joint):
    return joint.groupBy("provincia", "ciclista_id") \
                .agg(sum("longitud").alias("total_km"))


def getAgregatedKmDay(joint):
    return joint.groupBy("ciclista_id", "fecha") \
                .agg(sum("longitud").alias("por_dia")) \
                .groupBy("ciclista_id") \
                .agg(avg("por_dia").alias("promedio_diario"))


def topNParticionado(n, df, columnPartition, orderedBy):
    window = Window.partitionBy(columnPartition).orderBy(col(orderedBy).desc())
    return df \
        .fillna(value=0, subset=[orderedBy]) \
        .withColumn("row", row_number().over(window)) \
        .filter(col("row") <= n) \
        .drop("row")

def topN(n, df, orderedBy):
    return df \
        .fillna(value=0, subset=[orderedBy]) \
        .orderBy(col(orderedBy).desc()) \
        .limit(n)
