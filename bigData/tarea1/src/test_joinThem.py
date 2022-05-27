from . import reporte
from datetime import date
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField, StructType)

def test_one_record(spark_session):
    """Une un solo record de ciclista con una sola actividad"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 10)
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])
    actividades = spark_session.createDataFrame(
        [
            (1, 101110111, date(2022, 4, 25))
        ],
        ['ruta', 'ciclista', 'fecha'])
    
    expected = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 25), 1, 'Ruta1', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    actual = reporte.joinThem(ciclistas, rutas, actividades)
    assert expected.collect() == actual.collect()

def test_two_cyclists(spark_session):
    """Une 2 records de ciclista con 1 actividad cada uno"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela'),
            (202220222, 'Oscar', 'Alajuela')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 10)
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])
    actividades = spark_session.createDataFrame(
        [
            (1, 101110111, date(2022, 4, 24)),
            (1, 202220222, date(2022, 5, 15))
        ],
        ['ruta', 'ciclista', 'fecha'])
    
    expected = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (202220222, 'Oscar', 'Alajuela', 1, 202220222, date(2022, 5, 15), 1, 'Ruta1', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])
    
    actual = reporte.joinThem(ciclistas, rutas, actividades)
    actual = actual.sort(actual.ciclista_id.asc())
    assert expected.collect() == actual.collect()

def test_two_cyclists_no_activity_for_one(spark_session):
    """Une 2 records de ciclista con 1 actividad para solo el primero"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela'),
            (202220222, 'Oscar', 'Alajuela')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 10)
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])
    actividades = spark_session.createDataFrame(
        [
            (1, 101110111, date(2022, 4, 24))
        ],
        ['ruta', 'ciclista', 'fecha'])
    
    expected = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (202220222, 'Oscar', 'Alajuela', None, None, None, None, None, None)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    actual = reporte.joinThem(ciclistas, rutas, actividades)
    actual = actual.sort(actual.ciclista_id.asc())
    assert expected.collect() == actual.collect()

def test_two_cyclists_no_activity_at_all(spark_session):
    """Une 2 records de ciclista sin actividades"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela'),
            (202220222, 'Oscar', 'Alajuela')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 10)
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])
    actividades = spark_session.createDataFrame(
        [ ], # sin actividades
        schema = StructType([
                StructField('ruta',IntegerType(),False),
                StructField('ciclista',IntegerType(),False),
                StructField('fecha',DateType(),False)
            ])
        )
    
    expected = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', None, None, None, None, None, None),
            (202220222, 'Oscar', 'Alajuela', None, None, None, None, None, None)
        ],
        schema = StructType([
                StructField('ciclista_id',IntegerType(),False),
                StructField('nombre',StringType(),False),
                StructField('provincia',StringType(),False),
                StructField('ruta',IntegerType(),True),
                StructField('ciclista',IntegerType(),True),
                StructField('fecha',DateType(),True),
                StructField('ruta_id',IntegerType(),True),
                StructField('ruta_nombre',StringType(),True),
                StructField('longitud',IntegerType(),True)
            ])
    )
    
    actual = reporte.joinThem(ciclistas, rutas, actividades)
    actual = actual.sort(actual.ciclista_id.asc())
    assert expected.collect() == actual.collect()


def test_one_cyclist_2_activity_same_day(spark_session):
    """Une un solo record de ciclista con una sola actividad"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 10)
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])
    actividades = spark_session.createDataFrame(
        [
            (1, 101110111, date(2022, 4, 24)),
            (1, 101110111, date(2022, 4, 24)),
            (1, 101110111, date(2022, 5, 20))
        ],
        ['ruta', 'ciclista', 'fecha'])
    
    expected = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 5, 20), 1, 'Ruta1', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    print('expected')
    expected.show()
    
    actual = reporte.joinThem(ciclistas, rutas, actividades)
    actual = actual.sort(actual.ciclista_id.asc())
    print('actual')
    actual.show()
    assert expected.collect() == actual.collect()