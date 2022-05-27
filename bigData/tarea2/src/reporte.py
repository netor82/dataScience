from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, percentile_approx, sum, desc, count, lit, explode
from pyspark.sql.types import FloatType, StructField, StructType, StringType, ArrayType

def main(patron):
    df = cargarDatosDeArchivos(patron)
    
    df_totalViajes_1 = agregarTotalViajesPorLocacion(df, 'origen')
    df_totalViajes_2 = agregarTotalViajesPorLocacion(df, 'destino')
    df_totalViajes = unirYOrdenar(df_totalViajes_1, df_totalViajes_2)
    df_totalViajes.show()
    guardar(df_totalViajes, '/data/total_viajes.csv')
    
    df_totalIngresos_1 = agregarTotalIngresosPorLocacion(df, 'origen')
    df_totalIngresos_2 = agregarTotalIngresosPorLocacion(df, 'destino')
    df_totalIngresos = unirYOrdenar(df_totalIngresos_1, df_totalIngresos_2)
    df_totalIngresos.show()
    guardar(df_totalIngresos, '/data/total_ingresos.csv')
    
    df_metricas = getPersonaConMasKm(df) \
        .union( getPersonaConMasIngreso(df) ) \
        .union( getVentasPercentil(df, 0.25) ) \
        .union( getVentasPercentil(df, 0.50) ) \
        .union( getVentasPercentil(df, 0.75) ) \
        .union( getCodigoPostalOrigenConMasIngreso(df) ) \
        .union( getCodigoPostalDestinoConMasIngreso(df) )
    df_metricas.show()
    guardar(df_metricas, '/data/metricas.csv')


def cargarDatosDeArchivos(patron):
    spark = SparkSession.builder.appName("Create Report").getOrCreate()

    schema = StructType([StructField('identificador', StringType()),
                         StructField('viajes', ArrayType(StructType([
                                StructField('codigo_postal_origen', StringType()),
                                StructField('codigo_postal_destino', StringType()),
                                StructField('kilometros', StringType()),
                                StructField('precio_kilometro', StringType())
                            ])))
                        ])

    df = spark.read.option("multiline","true").schema(schema).json(patron)

    # Aplanar y cambiar tipos de datos
    df2 = df.select(df.identificador, explode(df.viajes).alias('listaViajes'))
    df3 = df2.select(df.identificador,
        df2.listaViajes.codigo_postal_origen.alias('origen'),
        df2.listaViajes.codigo_postal_destino.alias('destino'),
        df2.listaViajes.kilometros.cast(FloatType()).alias('km'),
        df2.listaViajes.precio_kilometro.cast(FloatType()).alias('precio_kilometro') )
    #df3.printSchema()
    df3 = df3.withColumn('costo', col('km') * col('precio_kilometro'))
    df3.show()

    return df3

def guardar(df, archivo):
    #df.write.csv(archivo) # lo guarda 'distribuido
    #df.repartition(1).write.csv(archivo) # lo guarda distribuido
    #df.repartition(1).write.format('com.databricks.spark.csv').save(archivo,header = 'true') # mismo
    df.coalesce(1).write.mode("overwrite").csv(archivo, header=False)
    #df.toPandas().to_csv(archivo) # necesita pandas configurado, se me hizo un enredo tratando de instalarlo en esa
    #df.save()

def agregarTotalViajesPorLocacion(df, columna):
    return df.groupBy(columna) \
        .agg(count(columna).alias('cantidad')) \
        .select(col(columna).alias('codigo_postal'), lit(columna).alias('tipo'), col('cantidad') )

def agregarTotalIngresosPorLocacion(df, columna):
    return df.groupBy(columna) \
        .agg(sum('costo').alias('ingresos')) \
        .select(col(columna).alias('codigo_postal'), lit(columna).alias('tipo'), col('ingresos') )

def unirYOrdenar(df, df2):
    return df.union(df2).orderBy(col('codigo_postal'))



def getPersonaConMasKm(df):
    return getPersonaConMas(df, 'km', 'persona_con_mas_kilometros')

def getPersonaConMasIngreso(df):
    return getPersonaConMas(df, 'costo', 'persona_con_mas_ingresos')

def getPersonaConMas(df, columna, nombreMetrica):
    return df.groupBy('identificador') \
        .agg(sum(columna).alias('sum')) \
        .orderBy(col('sum').desc()) \
        .select(lit(nombreMetrica).alias('tipo_metrica'), col('identificador').alias('valor')) \
        .limit(1)

def getVentasPercentil(df, percentil):
    return df.groupBy('identificador') \
        .agg(sum('costo').alias('sum')) \
        .select(lit(f"percentil_{int(percentil*100)}").alias('tipo_metrica'), \
                percentile_approx("sum", percentil).alias('valor'))


def getCodigoPostalOrigenConMasIngreso(df):
    return getCodigoPostalConMasIngreso(df, 'origen', 'codigo_postal_origen_con_mas_ingresos')

def getCodigoPostalDestinoConMasIngreso(df):
    return getCodigoPostalConMasIngreso(df, 'destino', 'codigo_postal_destino_con_mas_ingresos')

def getCodigoPostalConMasIngreso(df, tipo, nombreMetrica):
    return df.groupBy(tipo) \
        .agg(sum('costo').alias('sum')) \
        .orderBy(col('sum').desc()) \
        .select(lit(nombreMetrica).alias('tipo_metrica'), col(tipo).alias('valor')) \
        .limit(1)
