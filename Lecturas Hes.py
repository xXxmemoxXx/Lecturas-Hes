import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# Configuración de la página
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# --- CONFIGURACIÓN DE CONEXIÓN ---
# Se recomienda usar st.secrets en Streamlit Cloud para mayor seguridad
DB_HOST = "miaa.mx"
DB_USER = "miaamx_telemetria2"
DB_PASS = "bWkrw1Uum1O&"
DB_NAME = "miaamx_telemetria2"

@st.cache_resource
def get_connection():
    # Escapar el password por caracteres especiales como '&'
    password_escaped = urllib.parse.quote_plus(DB_PASS)
    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{password_escaped}@{DB_HOST}/{DB_NAME}")
    return engine

def main():
    st.title("📊 Visualizador de Telemetría HES - MIAA")
    st.sidebar.header("Filtros de Búsqueda")

    # 1. Conexión a la base de datos
    try:
        engine = get_connection()
    except Exception as e:
        st.error(f"Error al conectar a la base de datos: {e}")
        return

    # 2. Filtros en la barra lateral
    medidor_input = st.sidebar.text_input("Filtrar por Medidor (ID)", "")
    
    # 3. Definición de la Consulta SQL (Tu consulta personalizada)
    query = f"""
    SELECT 
        t1.Medidor, t1.ID, t1.Consumo_diario, t1.Fecha, t1.Lectura,
        t1.Predio, t1.MetodoID_API, t1.ClienteID_API, t1.Nombre,
        t1.Domicilio, t1.Colonia, t1.Nivel, t1.Situacion_comercial,
        t1.Giro, t1.Marca, t1.Modelo, t1.Primer_instalacion,
        t1.Distrito, t1.Sector, t1.Latitud, t1.Longitud,
        agg.Volumen_Mensual, agg.Ultima_Fecha, agg.Ultima_Lectura
    FROM HES t1
    LEFT JOIN (
        SELECT 
            Medidor, 
            YEAR(Fecha) AS Anio, 
            MONTH(Fecha) AS Mes,
            SUM(Consumo_diario) AS Volumen_Mensual,
            MAX(Fecha) AS Ultima_Fecha,
            MAX(Lectura) AS Ultima_Lectura
        FROM HES
        GROUP BY Medidor, YEAR(Fecha), MONTH(Fecha)
    ) agg 
        ON t1.Medidor = agg.Medidor 
        AND YEAR(t1.Fecha) = agg.Anio 
        AND MONTH(t1.Fecha) = agg.Mes
    """

    # Añadir filtro dinámico si el usuario escribe un medidor
    if medidor_input:
        query += f" WHERE t1.Medidor = '{medidor_input}'"
    
    query += " ORDER BY t1.Medidor, t1.Fecha DESC LIMIT 1000;" # Limitado para rendimiento

    # 4. Ejecución y Visualización
    if st.sidebar.button("Consultar Datos"):
        with st.spinner('Consultando base de datos...'):
            try:
                df = pd.read_sql(query, engine)
                
                if df.empty:
                    st.warning("No se encontraron registros.")
                else:
                    # Métricas Rápidas
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Registros", len(df))
                    col2.metric("Consumo Promedio", f"{df['Consumo_diario'].mean():.2f} m3")
                    col3.metric("Medidores Únicos", df['Medidor'].nunique())

                    # Mostrar Tabla de Datos
                    st.subheader("Registros Detallados")
                    st.dataframe(df, use_container_width=True)

                    # Mapa (Si hay datos de latitud y longitud)
                    if 'Latitud' in df.columns and 'Longitud' in df.columns:
                        st.subheader("📍 Ubicación de Medidores")
                        map_data = df[['Latitud', 'Longitud']].dropna().rename(columns={'Latitud': 'lat', 'Longitud': 'lon'})
                        st.map(map_data)

            except Exception as e:
                st.error(f"Error en la consulta: {e}")

if __name__ == "__main__":
    main()
