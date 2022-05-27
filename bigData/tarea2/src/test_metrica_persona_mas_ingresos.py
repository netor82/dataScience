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
        [('persona_con_mas_ingresos', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

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
        [('persona_con_mas_ingresos', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

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
        [('persona_con_mas_ingresos', 'B')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

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
        [('persona_con_mas_ingresos', 'B')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_2_choferes_version2(spark_session):
    """mismo destino, diferentes conductores, igual al anterior con diferente resultado"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10003', 3.0, 30.0, 90.0),
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('B', '10003', '10001', 3.0, 30.0, 90.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_ingresos', 'A')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios(spark_session):
    """mismo destino, diferentes conductores, igual al anterior con diferente resultado"""
    df = spark_session.createDataFrame(
        [
            ('B', '10012', '10002', 1.0, 10.0, 11.0),
            ('A', '10010', '10001', 1.0, 20.0, 20.0),
            ('C', '10013', '10003', 1.0, 30.0, 30.0),
            ('B', '10014', '10001', 1.0, 10.0, 10.0),
            ('A', '10015', '10002', 1.0, 40.0, 40.0),
            ('C', '10016', '10003', 1.0, 50.0, 50.0),
            ('B', '10016', '10001', 1.0, 60.0, 60.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('persona_con_mas_ingresos', 'B')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getPersonaConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

