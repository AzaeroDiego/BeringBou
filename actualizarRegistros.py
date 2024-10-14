import pyodbc
import pandas as pd
from sqlalchemy import create_engine  # Importa el motor de SQLAlchemy

class DatabaseConnection:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}')
            self.cursor = self.conn.cursor()
            print("Conexión exitosa a la base de datos.")
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute_query(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"Error ejecutando la consulta: {e}")
    
    def fetch_data(self, query):
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Error al obtener datos: {e}")
            return pd.DataFrame()

# Función para limpiar números de teléfono
def limpiar_numero(x):
    return ''.join([c for c in x if c.isdigit()])

# Crear el motor de conexión a SQL Server con SQLAlchemy
def get_engine(server, database, username, password):
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
    return engine

def actualizar_registros(db):
    # Obtener los registros existentes
    query = "SELECT evento, dni FROM MensajesWhatsApp"
    registros_existentes = db.fetch_data(query)
    
    # Supongamos que ya tienes tu archivo CSV cargado como DataFrame 'df'
    archivos = rf"C:\Users\azaer\Downloads\evento.csv"
    df = pd.read_csv(archivos, sep=';', encoding='latin1', dtype=str)
    
    # Preprocesar DataFrame
    df['dni'] = df['dni'].str.zfill(8).str.strip().str[0:8]
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d', errors='coerce')
    df['numero'] = df['numero'].apply(limpiar_numero)
    df = df.dropna(subset=['fecha', 'dni', 'evento', 'nombre'])
    
    # Convertir los registros existentes a un conjunto de tuplas para comparación
    registros_existentes_set = set(registros_existentes[['evento', 'dni']].apply(tuple, axis=1))

    # Filtrar los nuevos registros que no están en los registros existentes
    df_nuevos_registros = df[~df[['evento', 'dni']].apply(tuple, axis=1).isin(registros_existentes_set)]

    # Eliminar la columna 'fecha_registro' si existe
    if 'fecha_registro' in df_nuevos_registros.columns:
        df_nuevos_registros.drop(columns=['fecha_registro'], inplace=True)

    # Insertar nuevos registros en la base de datos usando SQLAlchemy
    if not df_nuevos_registros.empty:
        engine = get_engine(db.server, db.database, db.username, db.password)  # Usar el motor de SQLAlchemy
        df_nuevos_registros.to_sql('MensajesWhatsApp', engine, if_exists='append', index=False)
        print(f"Se insertaron {len(df_nuevos_registros)} nuevos registros.")

        # Actualizar la columna Recurrencia DNI_CEL
        actualizar_recurrencia(db)
    else:
        print("No hay nuevos registros para insertar.")

def actualizar_recurrencia(db):
    # Actualiza la columna RecurrenciaDNI_CEL
    query = """
    UPDATE MensajesWhatsApp
    SET RecurrenciaDNI_CEL = (
        SELECT CAST(COUNT(*) AS VARCHAR) + '|' + CAST(
            (SELECT COUNT(*) 
             FROM MensajesWhatsApp mw2 
             WHERE mw2.numero = MensajesWhatsApp.numero)
            AS VARCHAR)
        FROM MensajesWhatsApp mw
        WHERE mw.dni = MensajesWhatsApp.dni
    ),
    -- Actualizar todas las columnas excepto "Mensaje Enviado" si está repetido
    -- y si "Mensaje Enviado" es NULL o NO, se actualiza también
    MensajesWhatsApp = COALESCE(mw_actualizado.Mensaje_Enviado, MensajesWhatsApp.Mensaje_Enviado)
    FROM MensajesWhatsApp mw_actualizado
    WHERE mw_actualizado.dni = MensajesWhatsApp.dni 
    AND mw_actualizado.numero = MensajesWhatsApp.numero
    """
    db.execute_query(query)
    print("RecurrenciaDNI_CEL actualizada.")

