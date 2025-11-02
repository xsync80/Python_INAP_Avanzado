from pymongo import MongoClient
from bson.objectid import ObjectId  # Necesario para manejar ObjectId
from pprint import pprint

# --- CONFIGURACIÓN DE TU BASE DE DATOS Y COLECCIÓN ---
DB_NAME = 'bd'
COLLECTION_NAME = 'mi_coleccion'

# --- DOCUMENTOS DE EJEMPLO (para que puedas probar el script si la colección está vacía) ---
# Puedes comentar o eliminar esta sección si ya tienes tus propios datos
DOCUMENTOS_EJEMPLO = [
    {
        '_id': ObjectId('68e3f02de8afb4fa145c84ed'),
        'COD': "ECP355642",
        'Nombre': "Castilla y León. Tamaño medio del hogar. Número. ",
        'T3_Unidad': "Hogares",
        'T3_Escala': "",
        'MetaData': [{'Id': 9083, 'T3_Variable': "Comunidades y Ciudades Autónomas", 'Nombre': "Castilla y León",
                      'Codigo': "07"}],
        'Data': [
            {'Fecha': "2025-07-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de julio de",
             'Anyo': 2025, 'Valor': 2.24},
            {'Fecha': "2025-04-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de abril de",
             'Anyo': 2025, 'Valor': 2.24},
            {'Fecha': "2025-01-01T00:00:00.000+01:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de enero de",
             'Anyo': 2025, 'Valor': 2.25}
        ]
    },
    {
        '_id': ObjectId('68e3f02de8afb4fa145c84ee'),
        'COD': "ECP355643",
        'Nombre': "Cantabria. Tamaño medio del hogar. Número. ",
        'T3_Unidad': "Hogares",
        'T3_Escala': "",
        'MetaData': [
            {'Id': 9084, 'T3_Variable': "Comunidades y Ciudades Autónomas", 'Nombre': "Cantabria", 'Codigo': "06"}],
        'Data': [
            {'Fecha': "2025-07-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de julio de",
             'Anyo': 2025, 'Valor': 2.30},
            {'Fecha': "2025-04-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de abril de",
             'Anyo': 2025, 'Valor': 2.31}
        ]
    },
    {
        '_id': ObjectId('68e3f02de8afb4fa145c84ef'),
        'COD': "ECP355644",
        'Nombre': "Aragón. Tamaño medio del hogar. Número. ",
        'T3_Unidad': "Hogares",
        'T3_Escala': "",
        'MetaData': [
            {'Id': 9085, 'T3_Variable': "Comunidades y Ciudades Autónomas", 'Nombre': "Aragón", 'Codigo': "02"}],
        'Data': [
            {'Fecha': "2025-07-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de julio de",
             'Anyo': 2025, 'Valor': 2.35},
            {'Fecha': "2025-04-01T00:00:00.000+02:00", 'T3_TipoDato': "Provisional", 'T3_Periodo': "1 de abril de",
             'Anyo': 2025, 'Valor': 2.36}
        ]
    }
]


def obtener_comunidad_mayor_valor_julio_2025(db_name, collection_name):
    """
    Se conecta a MongoDB, busca los datos de julio de 2025 y encuentra
    la comunidad con el mayor valor.
    """
    client = None
    try:
        client = MongoClient('mongodb://localhost:27017/')
        collection = client[db_name][collection_name]
        print(f"Conectado a la colección '{collection_name}' en '{db_name}'.")

        # Usamos $elemMatch para buscar dentro del array 'Data' y filtramos por 'Anyo' y 'T3_Periodo'
        # También nos aseguramos de que 'Valor' sea un tipo numérico ($type: 1, 2, 8, 16, 18, 19)
        # para evitar comparar valores nulos o de texto por error.
        query = {
            'Data': {
                '$elemMatch': {
                    'Anyo': 2025,
                    'T3_Periodo': '1 de julio de',
                    'Valor': {'$type': ['double', 'int', 'long', 'decimal', 'float']}
                    # Asegurarse de que Valor es numérico
                }
            }
        }

        # Proyección para obtener solo los campos que necesitamos: Nombre y Data.Valor
        # Usamos $project y $unwind para trabajar con los elementos del array 'Data' individualmente
        # y luego $match para filtrar solo los de julio de 2025.
        pipeline = [
            {'$match': query},  # Primero filtramos los documentos relevantes
            {'$unwind': '$Data'},  # Desestructura el array 'Data' en documentos individuales
            {'$match': {  # Filtramos los elementos de 'Data' específicos
                'Data.Anyo': 2025,
                'Data.T3_Periodo': '1 de julio de',
                'Data.Valor': {'$type': ['double', 'int', 'long', 'decimal', 'float']}
            }},
            {'$project': {  # Proyectamos solo los campos necesarios
                '_id': 0,  # Excluir el _id
                'NombreComunidad': '$Nombre',  # Renombrar 'Nombre' a 'NombreComunidad'
                'ValorJulio2025': '$Data.Valor'  # Extraer el valor específico
            }},
            {'$sort': {'ValorJulio2025': -1}},  # Ordenar por Valor de forma descendente
            {'$limit': 1}  # Tomar solo el primer (mayor) resultado
        ]

        result = list(collection.aggregate(pipeline))

        if result:
            comunidad_max = result[0]
            print("\n--- Comunidad con el mayor valor en julio de 2025 ---")
            pprint(comunidad_max)
            return comunidad_max
        else:
            print("\nNo se encontraron datos para julio de 2025 con valores numéricos.")
            return None

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None
    finally:
        if client:
            client.close()
            print("Conexión a MongoDB cerrada.")


# --- Ejecución del script ---
if __name__ == "__main__":
    # --- LÓGICA OPCIONAL DE INSERCIÓN DE EJEMPLOS PARA PRUEBAS ---
    try:
        client_temp = MongoClient('mongodb://localhost:27017/')
        collection_temp = client_temp[DB_NAME][COLLECTION_NAME]

        inserted_count = 0
        for doc in DOCUMENTOS_EJEMPLO:
            if collection_temp.find_one({'_id': doc['_id']}) is None:
                collection_temp.insert_one(doc)
                inserted_count += 1
        if inserted_count > 0:
            print(f"Se insertaron {inserted_count} documentos de ejemplo.")
        else:
            print("Los documentos de ejemplo ya existen o no se insertó ninguno nuevo.")
    except Exception as e:
        print(f"No se pudieron insertar documentos de ejemplo (¿MongoDB no está corriendo?): {e}")
    finally:
        if 'client_temp' in locals() and client_temp:
            client_temp.close()
        print("Preparación de datos completada.")
    # --- FIN DE LA LÓGICA OPCIONAL DE INSERCIÓN ---

    # Llamar a la función principal para obtener el resultado
    comunidad_con_mayor_valor = obtener_comunidad_mayor_valor_julio_2025(DB_NAME, COLLECTION_NAME)

    if comunidad_con_mayor_valor:
        print("\nResultado final:")
        print(
            f"La comunidad con el mayor valor en julio de 2025 es '{comunidad_con_mayor_valor.get('NombreComunidad')}' con un valor de {comunidad_con_mayor_valor.get('ValorJulio2025')}.")
    else:
        print("No se pudo determinar la comunidad con el mayor valor.")