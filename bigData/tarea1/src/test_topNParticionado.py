from . import reporte

def test_basico(spark_session):
    """Top 1 de 1"""
    df = spark_session.createDataFrame(
        [
            ('A', 10)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(1, df, 'Id', 'Valor')

    print('expected')
    df.show()
    
    print('actual')
    actual.show()

    assert df.collect() == actual.collect()

def test_top_5_de_1_solo_record(spark_session):
    """Top 5 de 1, devuelve el único record"""
    df = spark_session.createDataFrame(
        [
            ('A', 10)
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('A', 10)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(5, df, 'Id', 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_1_de_2_mismo_tipo(spark_session):
    """Top 1 de 2 del mismo tipo, toma solo el mayor"""
    df = spark_session.createDataFrame(
        [
            ('A', 10),
            ('A', 3)
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('A', 10)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(1, df, 'Id', 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_1_de_2_al_reves(spark_session):
    """Top 1 de 2 del mismo tipo, entrada en orden inverso"""
    df = spark_session.createDataFrame(
        [
            ('A', 3),
            ('A', 10)
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('A', 10)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(1, df, 'Id', 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_1_de_2_tipos(spark_session):
    """Top 1 de 2 tipos distintos"""
    df = spark_session.createDataFrame(
        [
            ('A', 3),
            ('B', 10)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(1, df, 'Id', 'Valor')

    print('expected')
    df.show()
    
    print('actual')
    actual.show()

    assert df.collect() == actual.collect()

def test_2_de_5_mismo_tipo(spark_session):
    """Top 2 de 5 del mismo tipo, salida ordenada"""
    df = spark_session.createDataFrame(
        [
            ('A', 3),
            ('A', 1),
            ('A', 8),
            ('A', 6),
            ('A', 10)
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('A', 10),
            ('A', 8)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(2, df, 'Id', 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_2_de_2_tipos(spark_session):
    """Top 2 de 5 records de 2 tipos, salida ordenada"""
    df = spark_session.createDataFrame(
        [
            ('A', 3),
            ('A', 1),
            ('A', 8),
            ('A', 6),
            ('A', 10),

            ('B', 23),
            ('B', 21),
            ('B', 28),
            ('B', 26),
            ('B', 20),
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('A', 10),
            ('A', 8),
            ('B', 28),
            ('B', 26)
        ],
        ['Id', 'Valor'])

    actual = reporte.topNParticionado(2, df, 'Id', 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()