from . import reporte

def test_4_records_percentil_0(spark_session):
    """Solo 1 record"""
    df = spark_session.createDataFrame(
        [
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('D', '10000', '10001', 4.0, 100.0, 400.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_0', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.0)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_percentil_25(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_25', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.25)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_percentil_25(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_25', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.25)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_4_records_percentil_25(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0),
            ('D', '10000', '10001', 4.0, 100.0, 400.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_25', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.25)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_4_records_agrupables_percentil_25(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 50.0, 50.0),
            ('A', '10000', '10001', 1.0, 50.0, 50.0),
            ('C', '10000', '10001', 1.5, 100.0, 150.0),
            ('C', '10000', '10001', 1.5, 100.0, 150.0),
            ('D', '10000', '10001', 2.0, 100.0, 200.0),
            ('D', '10000', '10001', 2.0, 100.0, 200.0),
            ('B', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_25', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.25)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_varios_records_percentil_25(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 25.0, 25.0), #A 100
            ('A', '10000', '10001', 1.0, 25.0, 25.0),
            ('A', '10000', '10001', 1.0, 50.0, 50.0),
            ('B', '10000', '10001', 1.0, 100.0, 100.0), #B 200
            ('B', '10000', '10001', 1.0, 100.0, 100.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0), #C 300
            ('D', '10000', '10001', 1.0, 100.0, 100.0), #D 400
            ('D', '10000', '10001', 1.0, 100.0, 100.0),
            ('D', '10000', '10001', 1.0, 100.0, 100.0),
            ('D', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_25', 100.0)], #A
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.25)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_1_record_percentil_50(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_50', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.50)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_records_percentil_50(spark_session):
    df = spark_session.createDataFrame(
        [
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_50', 100.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.50)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_percentil_50(spark_session):
    df = spark_session.createDataFrame(
        [
            ('C', '10000', '10001', 3.0, 100.0, 300.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_50', 200.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.50)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_4_records_percentil_50(spark_session):
    df = spark_session.createDataFrame(
        [
            ('D', '10000', '10001', 4.0, 100.0, 400.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_50', 200.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.50)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_3_records_percentil_75(spark_session):
    df = spark_session.createDataFrame(
        [
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_75', 300.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.75)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_4_records_percentil_75(spark_session):
    df = spark_session.createDataFrame(
        [
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('D', '10000', '10001', 4.0, 100.0, 400.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_75', 300.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 0.75)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_4_records_percentil_100(spark_session):
    df = spark_session.createDataFrame(
        [
            ('D', '10000', '10001', 4.0, 100.0, 400.0),
            ('A', '10000', '10001', 1.0, 100.0, 100.0),
            ('B', '10000', '10001', 2.0, 100.0, 200.0),
            ('C', '10000', '10001', 3.0, 100.0, 300.0)
        ],
        ['identificador', 'origen', 'destino', 'km', 'precio_kilometro', 'costo'])

    expected = spark_session.createDataFrame(
        [('percentil_100', 400.0)],
        ['tipo_metrica', 'valor'])

    actual = reporte.getVentasPercentil(df, 1.0)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()