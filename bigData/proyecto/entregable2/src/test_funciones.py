from datetime import date, datetime
from decimal import Decimal
from pyspark.sql.types import (StringType, DateType, DecimalType, FloatType, IntegerType,
                               StructField, StructType)

from . import Proyecto

def test_crearDfFichaFinanciera(spark_session):
    entrada = [ ('A', datetime(2020, 1, 2), 1.4) ]

    expected = spark_session.createDataFrame(
        data = [
            ('A', date(2020, 1, 2), float(1.4))
        ],
        schema = StructType([
                StructField("cod", StringType()),
                StructField("mes", DateType()),
                StructField("utilidad", FloatType())
    ]))

    actual = Proyecto.crearDfFichaFinanciera(entrada)

    print('expected')
    expected.printSchema()
    
    print('actual')
    actual.printSchema()

    assert expected.collect() == actual.collect()


def test_crearDfCartera(spark_session):
    entrada = [ ('A', date(2020, 2, 1), 1, 1, "CRC", Decimal(10), Decimal(20), Decimal(30), Decimal(40), Decimal(50), Decimal(60)) ]

    expected = spark_session.createDataFrame(
        data = [
            ('A', datetime(2020, 2, 1), 1, 1, "CRC", Decimal(10), Decimal(20), Decimal(30), Decimal(40), Decimal(50), Decimal(60))
        ],
        schema = StructType([
                StructField("cod", StringType()),
                StructField("mes", DateType()),
                StructField("tipoTitulo", IntegerType()),
                StructField("sector", IntegerType()),
                StructField("moneda", StringType()),
                StructField("premio", DecimalType(12, 2)),
                StructField("costo", DecimalType(12, 2)),
                StructField("ganacias", DecimalType(12, 2)),
                StructField("montoDeterioro", DecimalType(12, 2)),
                StructField("valorMercado", DecimalType(12, 2)),
                StructField("valorMercadoCol", DecimalType(12, 2))
    ]))

    actual = Proyecto.crearDfCartera(entrada)

    print('expected')
    expected.printSchema()
    
    print('actual')
    actual.printSchema()

    assert expected.collect() == actual.collect()


def test_contarNulos(spark_session):
   df = spark_session.createDataFrame(
       [
           ('AXY', '10000', 100.0),
           ('AXX', None, 100.0),
           ('AXO', None, None)
       ],
       ['A', 'B', 'C'])

   expected = spark_session.createDataFrame(
       [(0, 2, 1)],
       ['A', 'B', 'C'])

   actual = Proyecto.contarNulos(df)

   print('expected')
   expected.show()
   
   print('actual')
   actual.show()

   assert expected.collect() == actual.collect()

def test_deTextACodigos(spark_session):
   """ el código lo asigna por frecuencia, siendo el 0 el de mayor frecuencia"""
   df = spark_session.createDataFrame(
       [
           ('Cod1', 'CRC'),
           ('Cod1', 'USD'),
           ('Cod2', 'USD')
       ],
       ['cod', 'moneda'])

   expected = spark_session.createDataFrame(
       [
           ('Cod1', 'CRC', 0, 1),
           ('Cod1', 'USD', 0, 0),
           ('Cod2', 'USD', 1, 0)
       ],
       ['cod', 'moneda', 'cod2', 'moneda_2'])

   actual = Proyecto.deTextACodigos(df)

   print('expected')
   expected.show()
   
   print('actual')
   actual.show()

   assert expected.collect() == actual.collect()

def test_calcularColones(spark_session):
   """El tipo de cambio es $1 = c500"""
   df = spark_session.createDataFrame(
       [
           (Decimal(1), Decimal(500), Decimal(2), Decimal(3), Decimal(4), Decimal(5)),
           (Decimal(500), Decimal(500), Decimal(2), Decimal(3), Decimal(4), Decimal(5))
       ],
       ['valorMercado', 'valorMercadoCol', 'premio', 'costo', 'ganacias', 'montoDeterioro'])

   expected = spark_session.createDataFrame(
       [
           (Decimal(1), Decimal(500), Decimal(2), Decimal(3), Decimal(4), Decimal(5),
            Decimal(500), Decimal(1000), Decimal(1500), Decimal(2000), Decimal(2500)),

            #este record está en colones, no cambia
           (Decimal(500), Decimal(500), Decimal(2), Decimal(3), Decimal(4), Decimal(5),
            Decimal(1), Decimal(2), Decimal(3), Decimal(4), Decimal(5))
       ],
       ['valorMercado', 'valorMercadoCol', 'premio', 'costo', 'ganacias', 'montoDeterioro',
        'tipoCambio', 'premioCol', 'costoCol', 'ganaciasCol', 'montoDeterioroCol'])

   actual = Proyecto.calcularColones(df)

   print('expected')
   expected.show()
   
   print('actual')
   actual.show()

   assert expected.collect() == actual.collect()

def test_margenUtilidadSiguienteMes(spark_session):
   """se 'pierde' un record por código"""
   df = spark_session.createDataFrame(
       [
           ('A', date(2020, 1, 1), float(1.0)),
           ('A', date(2020, 2, 1), float(2.0)),
           ('A', date(2020, 3, 1), float(3.0)),
           ('B', date(2020, 3, 1), float(1.0))
       ],
       ['cod', 'mes', 'utilidad'])

   expected = spark_session.createDataFrame(
       [
           ('A', date(2020, 1, 1), date(2020, 2, 1), float(1.0), float(2.0)),
           ('A', date(2020, 2, 1), date(2020, 3, 1), float(2.0), float(3.0))
       ],
       ['cod', 'mes', 'sig_mes', 'utilidad', 'label'])

   actual = Proyecto.margenUtilidadSiguienteMes(df)

   print('expected')
   expected.show()
   
   print('actual')
   actual.show()

   assert expected.collect() == actual.collect()

def test_unirParaAnalizar(spark_session):
    """mismo destino, diferentes conductores, igual al anterior con diferente resultado"""

    dfCartera = spark_session.createDataFrame(
        [
            ('A', date(2020, 1, 1), 10, 20, 30, 0, Decimal(1), Decimal(2), Decimal(3), Decimal(4), Decimal(5)),
            ('A', date(2020, 1, 1), 10, 120, 30, 1, Decimal(500), Decimal(20), Decimal(30), Decimal(40), Decimal(50))
        ],
        [
            'cod', 'mes', 'cod2', 'tipoTitulo', 'sector', 'moneda_2', 
            'valorMercadoCol', 'premioCol', 'costoCol',
            'ganaciasCol', 'montoDeterioroCol'
        ]
    )

    dfUtilidades = spark_session.createDataFrame(
        [
            ('A', date(2020, 1, 1), float(1.0), float(2.0))
        ],
        ['cod', 'mes', 'utilidad', 'label']
    )


    expected = spark_session.createDataFrame(
        [
           ('A', date(2020, 1, 1), 10, 20, 30, 0, Decimal(1), Decimal(2), Decimal(3), Decimal(4), Decimal(5), float(1.0), float(2.0)),
           ('A', date(2020, 1, 1), 10, 120, 30, 1, Decimal(500), Decimal(20), Decimal(30), Decimal(40), Decimal(50), float(1.0), float(2.0))
        ],
        [
           'cod', 'mes', 'cod2', 'tipoTitulo', 'sector', 'moneda_2', 
           'valorMercadoCol', 'premioCol', 'costoCol',
           'ganaciasCol', 'montoDeterioroCol',
           'utilidad', 'label'
        ]
    )

    actual = Proyecto.unirParaAnalizar(dfCartera, dfUtilidades)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

