import sys
import reporte

if len(sys.argv) < 2:
    patronArchivo = 'persona*.json'
    print(f"Falta el argumento de patrón de archivo. Usando '{patronArchivo}'")
else:
    patronArchivo = sys.argv[1]
    print(f"Patrón para cargar archivos: '{patronArchivo}'")

reporte.main('/data/' + patronArchivo)