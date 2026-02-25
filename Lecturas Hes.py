import streamlit as st
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine
import urllib.parse
import folium
from streamlit_folium import folium_static

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="MIAA - Gestión Hídrica Integral", layout="wide")

# --- CREDENCIALES ---
CONFIG_POSTGRES = {
    'user': 'map_tecnica',
    'password': 'M144.Tec',
    'host': 'ti.miaa.mx',
    'database': 'qgis',
    'port': '5432'
}

MYSQL_DATA = {
    'user': 'miaamx_telemetria2',
    'pass': 'bWkrw1Uum1O&',
    'host': 'miaa.mx',
    'db': 'miaamx_telemetria2'
}

# 2. FUNCIONES DE CONEXIÓN
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus(MYSQL_DATA['pass'])
    return create_engine(f"mysql+mysqlconnector://{MYSQL_DATA['user']}:{pwd}@{MYSQL_DATA['host']}/{MYSQL_DATA['db']}")

def get_postgres_conn():
    try:
        return psycopg2.connect(**CONFIG_POSTGRES)
    except Exception as e:
        st.error(f"Error Postgres: {e}")
        return None

# 3. OBTENCIÓN DE DATOS
@st.cache_data(ttl=600)
def fetch_sectors_geojson():
    conn = get_postgres_conn()
    if not conn: return []
    
    geojson_features = []
    query = """
        SELECT sector, "Pozos_Sector", "Poblacion", "Vol_Prod", "Superficie", "Long_Red", "U_Domesticos",
               ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
        FROM "Sectorizacion"."Sectores_hidr";
    """
    try:
        df = pd.read_sql(query, conn)
        for _, row in df.iterrows():
            feature = {
                'type': 'Feature',
                'geometry': json.loads(row['geojson_data']),
                'properties': {
                    'name': f"Sector {row['sector']}",
                    'popup': f"""<b>Sector:</b> {row['sector']}<br>
                                 <b>Población:</b> {row['Poblacion']}<br>
                                 <b>Vol. Producido:</b> {row['Vol_Prod']}"""
                }
            }
            geojson_features.append(feature)
    finally:
        conn.close()
    return geojson_features

@st.cache_data(ttl=300)
def fetch_telemetry():
    engine = get_mysql_engine()
    query = "SELECT Medidor, Latitud, Longitud, Consumo_diario, Colonia FROM HES ORDER BY Fecha DESC LIMIT 1000"
    df = pd.read_sql(query, engine)
    df['Latitud'] = pd.to_numeric(df['Latitud'], errors='coerce')
    df['Longitud'] = pd.to_numeric(df['Longitud'], errors='coerce')
    return df.dropna(subset=['Latitud', 'Longitud'])

# 4. INTERFAZ Y MAPA
st.title("🗺️ Tablero Maestro: Sectores y Telemetría")

# Carga de datos
with st.spinner("Cargando capas geográficas..."):
    sectores = fetch_sectors_geojson()
    medidores = fetch_telemetry()

# Métricas rápidas
col1, col2, col3 = st.columns(3)
col1.metric("Sectores Activos", len(sectores))
col2.metric("Medidores en Red", len(medidores))
col3.metric("Consumo Promedio", f"{medidores['Consumo_diario'].mean():.2f} m3")

# Crear Mapa Folium
m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="cartodbpositron")

# Capa 1: Sectores (Polígonos de Postgres)
if sectores:
    folium.GeoJson(
        {'type': 'FeatureCollection', 'features': sectores},
        name="Sectores Hidráulicos",
        style_function=lambda x: {
            'fillColor': '#00FFFF',
            'color': '#008B8B',
            'weight': 1,
            'fillOpacity': 0.3
        },
        tooltip=folium.GeoJsonTooltip(fields=['name'], labels=False),
        popup=folium.GeoJsonPopup(fields=['popup'], labels=False)
    ).add_to(m)

# Capa 2: Medidores (Puntos de MySQL)
for _, row in medidores.iterrows():
    # Color del punto según consumo
    color = "green" if row['Consumo_diario'] < 1.0 else "orange" if row['Consumo_diario'] < 5.0 else "red"
    
    folium.CircleMarker(
        location=[row['Latitud'], row['Longitud']],
        radius=4,
        color=color,
        fill=True,
        fill_opacity=0.7,
        popup=f"Medidor: {row['Medidor']}<br>Consumo: {row['Consumo_diario']} m3"
    ).add_to(m)

# Renderizar Mapa
folium_static(m, width=1300, height=600)

# Tabla de datos inferior
st.subheader("Detalle de Lecturas")
st.dataframe(medidores, use_container_width=True)
