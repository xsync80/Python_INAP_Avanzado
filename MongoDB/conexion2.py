from pymongo import MongoClient
from bson.objectid import ObjectId
from pprint import pprint


def obtener_datos_coleccion(nombre_bd='bd', nombre_coleccion='mi_coleccion'):
    """
    Se conecta a MongoDB, itera sobre los documentos de una colección
    y extrae información específica, especialmente del array 'Data'.
    """
    client = None
    try:
        # 1. Conexión a MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client[nombre_bd]
        collection = db[nombre_coleccion]

        print(f"Conectado a la base de datos '{nombre_bd}' y colección '{nombre_coleccion}'.")

        # Lista para almacenar todos los datos extraídos
        todos_los_datos_extraidos = []

        # 2. Iterar sobre los documentos de la colección
        # collection.find({}) significa "todos los documentos"
        for document in collection.find({}):
            # Obtener campos de nivel superior
            doc_id = str(document.get('_id'))
            cod = document.get('COD')
            nombre_general = document.get('Nombre')
            t3_unidad = document.get('T3_Unidad')
            t3_escala = document.get('T3_Escala')

            # Obtener datos de MetaData (si existen y tienen la estructura esperada)
            meta_data_info = {}
            if 'MetaData' in document and isinstance(document['MetaData'], list):
                for item in document['MetaData']:
                    if isinstance(item, dict) and 'T3_Variable' in item and 'Nombre' in item:
                        # Usar T3_Variable como clave y Nombre como valor para un resumen conciso
                        meta_data_info[item['T3_Variable']] = item['Nombre']

            # Obtener datos del array 'Data'
            if 'Data' in document and isinstance(document['Data'], list):
                for data_item in document['Data']:
                    if isinstance(data_item, dict):
                        # Crear un diccionario para cada entrada de 'Data' con su contexto
                        datos_entry = {
                            '_id_documento_padre': doc_id,
                            'COD_padre': cod,
                            'Nombre_general_padre': nombre_general,
                            'T3_Unidad_padre': t3_unidad,
                            'T3_Escala_padre': t3_escala,
                            'MetaData_resumen': meta_data_info,  # Incluir el resumen de MetaData
                            'Fecha': data_item.get('Fecha'),
                            'T3_TipoDato': data_item.get('T3_TipoDato'),
                            'T3_Periodo': data_item.get('T3_Periodo'),
                            'Anyo': data_item.get('Anyo'),
                            'Valor': data_item.get('Valor')
                        }
                        todos_los_datos_extraidos.append(datos_entry)
            else:
                print(f"Advertencia: Documento con ID {doc_id} no tiene un array 'Data' o está vacío.")

        print(f"\nSe extrajeron {len(todos_los_datos_extraidos)} entradas de datos.")
        return todos_los_datos_extraidos

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return []
    finally:
        # 3. Cerrar la conexión
        if client:
            client.close()
            print("Conexión a MongoDB cerrada.")


# --- Cómo usar la función ---
if __name__ == "__main__":
    # Asegúrate de que tu MongoDB esté corriendo.
    # Los nombres de la base de datos y la colección ahora coinciden con tu imagen.
    DB_NAME = 'bd'
    COLLECTION_NAME = 'mi_coleccion'

    # OPCIONAL: Insertar los documentos de ejemplo si no existen.
    # ¡Comenta estas líneas si ya tienes estos datos o quieres evitar duplicados!
    try:
        client_temp = MongoClient('mongodb://localhost:27017/')
        db_temp = client_temp[DB_NAME]
        collection_temp = db_temp[COLLECTION_NAME]

        # Intentar insertar el documento de Castilla y León
        if collection_temp.find_one({'_id': DOCUMENTO_EJEMPLO_CASTILLA_LEON['_id']}) is None:
            print(
                f"Insertando documento de ejemplo (Castilla y León) con ID {DOCUMENTO_EJEMPLO_CASTILLA_LEON['_id']}...")
            collection_temp.insert_one(DOCUMENTO_EJEMPLO_CASTILLA_LEON)
            print("Documento de ejemplo (Castilla y León) insertado.")
        else:
            print("El documento de ejemplo (Castilla y León) ya existe.")

        # Intentar insertar el documento de Cantabria
        if collection_temp.find_one({'_id': DOCUMENTO_EJEMPLO_CANTABRIA['_id']}) is None:
            print(f"Insertando documento de ejemplo (Cantabria) con ID {DOCUMENTO_EJEMPLO_CANTABRIA['_id']}...")
            collection_temp.insert_one(DOCUMENTO_EJEMPLO_CANTABRIA)
            print("Documento de ejemplo (Cantabria) insertado.")
        else:
            print("El documento de ejemplo (Cantabria) ya existe.")

    except Exception as e:
        print(f"No se pudieron insertar los documentos de ejemplo (¿MongoDB no está corriendo?): {e}")
    finally:
        if 'client_temp' in locals() and client_temp:
            client_temp.close()
            print("Conexión temporal para inserción cerrada.")

    # Llamar a la función principal para obtener los datos de la colección
    datos_extraidos = obtener_datos_coleccion(
        nombre_bd=DB_NAME,
        nombre_coleccion=COLLECTION_NAME
    )

    if datos_extraidos:
        print("\n--- Primeros 5 registros extraídos ---")
        for i, record in enumerate(datos_extraidos[:5]):
            print(f"Registro {i + 1}:")
            pprint(record)
            print("-" * 20)
    else:
        print("No se pudieron extraer datos o la colección está vacía.")