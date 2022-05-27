from . import reporte
from datetime import date


def test_basico(spark_session):
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            ('Alajuela', 101110111, 10)
        ],
        ['provincia', 'ciclista_id', 'total_km'])

    actual = reporte.getAgregatedProvincia(join)
    actual = actual.sort(actual.total_km.desc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

    
def test_una_provincia(spark_session):
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 5, 20), 1, 'Ruta1', 10),
            (202220222, 'Oscar', 'Alajuela', 1, 202220222, date(2022, 5, 15), 1, 'Ruta1', 10),
            (303330333, 'Marta', 'Alajuela', None, None, None, None, None, None)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            ('Alajuela', 101110111, 30),
            ('Alajuela', 202220222, 10),
            ('Alajuela', 303330333, None)
        ],
        ['provincia', 'ciclista_id', 'total_km'])

    actual = reporte.getAgregatedProvincia(join)
    actual = actual.sort(actual.total_km.desc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_dos_provincias(spark_session):
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 5, 20), 1, 'Ruta1', 10),
            (202220222, 'Oscar', 'San José', 1, 202220222, date(2022, 5, 15), 1, 'Ruta1', 10),
            (303330333, 'Marta', 'San José', None, None, None, None, None, None)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            ('Alajuela', 101110111, 30),
            ('San José', 202220222, 10),
            ('San José', 303330333, None)
        ],
        ['provincia', 'ciclista_id', 'total_km'])

    actual = reporte.getAgregatedProvincia(join)
    actual = actual.sort(actual.total_km.desc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()