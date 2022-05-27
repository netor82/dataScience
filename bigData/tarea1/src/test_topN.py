from . import reporte

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

    actual = reporte.topN(5, df, 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_top_2_de_3_records(spark_session):
    """Top 2 de 3, con entrada desordenada"""
    df = spark_session.createDataFrame(
        [
            ('B', 2),
            ('A', 1),
            ('C', 3),
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('C', 3),
            ('B', 2)
        ],
        ['Id', 'Valor'])

    actual = reporte.topN(2, df, 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_top_3_de_3_con_valor_nulo(spark_session):
    """Top 2 de 3, con entrada desordenada"""
    df = spark_session.createDataFrame(
        [
            ('A', 1),
            ('C', None),
            ('B', 2)
        ],
        ['Id', 'Valor'])

    expected = spark_session.createDataFrame(
        [
            ('B', 2),
            ('A', 1),
            ('C', 0)
        ],
        ['Id', 'Valor'])

    actual = reporte.topN(3, df, 'Valor')

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()