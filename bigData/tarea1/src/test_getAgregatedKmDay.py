from . import reporte
from datetime import date


def test_basico(spark_session):
    """Solo un record"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 10)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

    
def test_una_fecha_por_ciclista(spark_session):
    """Tres ciclistas, cada uno con solo un entrenamiento"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (202220222, 'Oscar', 'Alajuela', 1, 202220222, date(2022, 5, 15), 1, 'Ruta1', 10),
            (303330333, 'Marta', 'Alajuela', None, None, None, None, None, None)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 10),
            (202220222, 10),
            (303330333, None)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)
    actual = actual.sort(actual.ciclista_id.asc())

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_un_ciclista_dos_fechas(spark_session):
    """Mismo ciclista, dos fechas"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 5, 20), 1, 'Ruta1', 8)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 9)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_un_ciclista_dos_entradas_misma_fecha(spark_session):
    """mismo ciclista, misma fecha, debe dar promedio de ambas rutas"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 20),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 20)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 40)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_un_ciclista_dos_rutas_misma_fecha(spark_session):
    """mismo ciclista, misma fecha, debe dar promedio de ambas rutas"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 20),
            (101110111, 'Pedro', 'Alajuela', 2, 101110111, date(2022, 4, 24), 2, 'Ruta2', 10)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 30)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_un_ciclista_mixto_fechas(spark_session):
    """Dos fechas distintas, debe dar promedio por fecha"""
    join = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 20),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 4, 24), 1, 'Ruta1', 10),
            (101110111, 'Pedro', 'Alajuela', 1, 101110111, date(2022, 5, 25), 1, 'Ruta1', 8)
        ],
        ['ciclista_id', 'nombre', 'provincia', 'ruta', 'ciclista', 'fecha', 'ruta_id', 'ruta_nombre', 'longitud'])

    expected = spark_session.createDataFrame(
        [
            (101110111, 19)
        ],
        ['ciclista_id', 'promedio_diario'])

    actual = reporte.getAgregatedKmDay(join)

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

