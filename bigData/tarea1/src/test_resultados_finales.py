from . import reporte
from datetime import date

def createDF(spark_session):
    """Crea los datos para las pruebas"""
    ciclistas = spark_session.createDataFrame(
        [
            (101110111, 'Pedro', 'Alajuela'),
            (202220222, 'Oscar', 'Alajuela'),
            (303330333, 'Juan', 'Alajuela'),
            (404440444, 'Julanito', 'Alajuela'),
            (505550555, 'Gerardo', 'Heredia'),
            (606660666, 'Roberto', 'Heredia'),
            (707770777, 'Luis', 'Heredia')
        ],
        ['ciclista_id', 'nombre', 'provincia'])

    rutas = spark_session.createDataFrame(
        [
            (1, 'Ruta1', 3),
            (2, 'Ruta2', 5),
            (3, 'Ruta3', 7),
            (4, 'Ruta4', 11),
        ],
        ['ruta_id', 'ruta_nombre', 'longitud'])

    actividades = spark_session.createDataFrame(
        [
            (1, 101110111, date(2022, 4, 25)), #3+5 =  8
            (2, 101110111, date(2022, 4, 25)),
            (4, 101110111, date(2022, 5, 25)), #11 + 9 / 2 = 9.5 Promedio diario

            (1, 202220222, date(2022, 5, 25)),
            (1, 202220222, date(2022, 6, 25)),
            (1, 202220222, date(2022, 7, 25)),

            (4, 303330333, date(2022, 1, 25)),
            (4, 303330333, date(2022, 1, 28)),

            (2, 404440444, date(2022, 4, 25)),
            (2, 404440444, date(2022, 5, 25)),
            (3, 404440444, date(2022, 6, 25)),
            (3, 404440444, date(2022, 7, 25)),

            (1, 505550555, date(2022, 4, 20)),
            (3, 505550555, date(2022, 4, 22)),
            (4, 505550555, date(2022, 4, 25)),

            (2, 606660666, date(2022, 5, 25)),
            (1, 606660666, date(2022, 6, 25))

            # sin registros para ciclista 707770777
        ],
        ['ruta', 'ciclista', 'fecha'])

    return ciclistas, rutas, actividades
    
def test_top3_por_provincia(spark_session):
    """Alajuela tiene 4, solo muestra 3.
       Heredia tiene solo 2 con actividades, uno queda en Nulo"""
    ciclistas, rutas, actividades = createDF(spark_session)

    actual, _ = reporte.getTopNReports(3, ciclistas, rutas, actividades)

    expected = spark_session.createDataFrame(
        [
            ('Alajuela', 404440444, 24),
            ('Alajuela', 303330333, 22),
            ('Alajuela', 101110111, 19),
            ('Heredia', 505550555, 21),
            ('Heredia', 606660666, 8),
            ('Heredia', 707770777, 0) # --> gracias por registrarse :)
        ],
        ['provincia', 'ciclista_id', 'total_km'])

    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()

def test_top3_promedio_diario(spark_session):
    """Alajuela tiene 4, solo muestra 3.
       Heredia tiene solo 2 con actividades, uno queda en Nulo"""
    ciclistas, rutas, actividades = createDF(spark_session)

    _, actual = reporte.getTopNReports(3, ciclistas, rutas, actividades)

    expected = spark_session.createDataFrame(
        [
            (303330333, 11.0),
            (101110111, 9.5),
            (505550555, 7.0)
        ],
        ['ciclista_id', 'promedio_diario'])


    print('expected')
    expected.show()
    
    print('actual')
    actual.show()

    assert expected.collect() == actual.collect()