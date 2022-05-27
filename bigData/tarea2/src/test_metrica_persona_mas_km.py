from . import reporte
from pyspark.sql.functions import col

def test_1_record(spark_session):
    """Solo 1 record"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_kilometros', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasKm(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_mismo_chofer(spark_session):
    """2 records mismo chofer"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10000', '10002', 2.0, 100.0, 200.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_kilometros', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasKm(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_diferente_chofer(spark_session):
    """2 records con diferente chofer"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10002', 2.0, 100.0, 200.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_kilometros', 'B')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasKm(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_2_choferes(spark_session):
    """3 records al mismo destino, diferente precio final"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('B', '10003', '10001', 3.0, 30.0, 90.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_kilometros', 'B')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasKm(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_2_choferes_2(spark_session):
    """mismo destino, diferentes conductores, procesar destino"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10003', 3.0, 30.0, 30.0),
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('B', '10003', '10001', 3.0, 30.0, 90.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_kilometros', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasKm(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

