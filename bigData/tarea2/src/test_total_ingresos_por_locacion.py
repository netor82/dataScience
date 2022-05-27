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
            ('10000', 'origen', 100.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'origen')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_origen_diferente_costo(spark_session):
    """2 records condiferente costo final, procesar origen"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10000', '10002', 2.0, 100.0, 200.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10000', 'origen', 300.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'origen')

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
            ('10001', 'destino', 100.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_mismo_destino_diferente_costo(spark_session):
    """3 records al mismo destino, diferente precio final"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('A', '10002', '10001', 2.0, 20.0, 40.0),
            ('A', '10003', '10001', 3.0, 30.0, 90.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 140.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_mismo_destino_diferente_identificador(spark_session):
    """mismo destino, diferentes conductores, procesar destino"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10002', '10001', 1.0, 200.0, 200.0),
            ('C', '10003', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 400.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'destino')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios_destinos_mismo_costo(spark_session):
    """varios destinos, varios choferes, mismo costo del viaje"""
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
            ('10001', 'destino', 300.0),
            ('10002', 'destino', 200.0),
            ('10003', 'destino', 200.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'destino').orderBy(col('codigo_postal').asc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios_destinos_distinto_costo(spark_session):
    """varios destinos, varios choferes, varios costos"""
    df = spark_session.createDataFrame(
        [
            ('B', '10012', '10002', 1.0, 10.0, 10.0),
            ('A', '10010', '10001', 1.0, 20.0, 20.0),
            ('C', '10013', '10003', 1.0, 30.0, 30.0),
            ('C', '10014', '10001', 1.0, 10.0, 10.0),
            ('C', '10015', '10002', 1.0, 40.0, 40.0),
            ('C', '10016', '10003', 1.0, 50.0, 50.0),
            ('C', '10016', '10001', 1.0, 60.0, 60.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [
            ('10001', 'destino', 90.0),
            ('10002', 'destino', 50.0),
            ('10003', 'destino', 80.0)
        ],
        ['codigo_postal', 'tipo', 'ingresos'])

    actual = reporte.agregarTotalIngresosPorLocacion(df, 'destino').orderBy(col('codigo_postal').asc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()