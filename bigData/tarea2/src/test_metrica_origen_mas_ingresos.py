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
        [('codigo_postal_origen_con_mas_ingresos', '10000')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_mismo_origen(spark_session):
    """2 records mismo código postal origen"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10000', '10002', 2.0, 100.0, 200.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('codigo_postal_origen_con_mas_ingresos', '10000')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_diferente_origen(spark_session):
    """2 records con diferente código postal origen"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('A', '10010', '10002', 2.0, 100.0, 200.0),
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('codigo_postal_origen_con_mas_ingresos', '10010')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_2_origenes(spark_session):
    """2 records sumados dan menos que uno único que generó más"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('B', '10010', '10001', 3.0, 30.0, 90.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('codigo_postal_origen_con_mas_ingresos', '10010')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_2_origenes_version2(spark_session):
    """igual al anterior con diferente resultado por el ingreso generado"""
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('A', '10000', '10001', 1.0, 10.0, 10.0),
            ('B', '10010', '10001', 1.0, 10.0, 10.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('codigo_postal_origen_con_mas_ingresos', '10000')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios_records(spark_session):
    """mismo destino, diferentes conductores, igual al anterior con diferente resultado"""
    df = spark_session.createDataFrame(
        [
            ('A', '10010', '10002', 1.0, 10.0, 11.0),
            ('B', '10011', '10001', 1.0, 20.0, 20.0),
            ('C', '10012', '10003', 1.0, 30.0, 30.0),
            ('D', '10010', '10001', 1.0, 10.0, 10.0),
            ('E', '10011', '10002', 1.0, 40.0, 40.0),
            ('F', '10012', '10003', 1.0, 50.0, 50.0),
            ('G', '10013', '10001', 1.0, 60.0, 60.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('codigo_postal_origen_con_mas_ingresos', '10012')],
        ['tipo_metrica', 'valor'])

    actual = reporte.getCodigoPostalOrigenConMasIngreso(df)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

