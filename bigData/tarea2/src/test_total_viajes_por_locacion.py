from . import reporte
from pyspark.sql.functions import col

def test_1_record_origen(spark_session):
    """Solo 1 record, procesar origen"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10000', 'origen', 1)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'origen')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_origen(spark_session):
    """2 records, procesar origen"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10000', '10002', 1.0, 100.0, 100.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10000', 'origen', 2)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'origen')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_1_record_destino(spark_session):
    """Solo 1 record, procesar destino"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 1)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_mismo_destino(spark_session):
    """3 records, procesar destino"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10002', '10001', 1.0, 100.0, 100.0),
            ('A', '10003', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 3)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_mismo_destino_diferente_identificador(spark_session):
    """3 records con diferente chofer, mismo destino"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10002', '10001', 1.0, 100.0, 100.0),
            ('C', '10003', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 3)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios_destinos(spark_session):
    """varios choferes, varios destinos"""
    df = spark_session.createDataFrame(
        [
            ('B', '10012', '10002', 1.0, 100.0, 100.0),
            ('A', '10010', '10001', 1.0, 100.0, 100.0),
            ('C', '10013', '10003', 1.0, 100.0, 100.0),
            ('C', '10014', '10001', 1.0, 100.0, 100.0),
            ('C', '10015', '10002', 1.0, 100.0, 100.0),
            ('C', '10016', '10003', 1.0, 100.0, 100.0),
            ('C', '10016', '10001', 1.0, 100.0, 100.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 3),
            ('10002', 'destino', 2),
            ('10003', 'destino', 2)
        ],
        ['codigo_postal', 'tipo', 'cantidad'])

    actual = reporte.agregarTotalViajesPorLocacion(df, 'destino').orderBy(col('codigo_postal').asc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()