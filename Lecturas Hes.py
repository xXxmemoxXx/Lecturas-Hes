import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 2. LÓGICA DE COLOR (SIMBOLOGÍA EXACTA)
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    lim = config.get(str(nivel).upper(), [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# 3. CARGA DE DATOS
mysql_engine = get_mysql_engine()
with st.sidebar:
    fecha_rango = st.date_input("Periodo", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        try:
            pg_conn = get_postgres_conn()
            df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', pg_conn)
            pg_conn.close()
        except: df_sec = pd.DataFrame()
    else: st.stop()

# 4. PROCESAMIENTO (Agregamos campos para el popup)
df_mapa = df_hes.groupby('Medidor').agg({
    'Consumo_diario': 'sum',
    'Lectura': 'last',
    'Latitud': 'first',
    'Longitud': 'first',
    'Nivel': 'first',
    'ClientID_API': 'first',
    'Nombre': 'first',
    'Predio': 'first',
    'Domicilio': 'first',
    'Colonia': 'first',
    'Giro': 'first',
    'Sector': 'first',
    'Metodoid_API': 'first',
    'Primer_instalacion': 'first',
    'Fecha': 'last'
}).reset_index()

# 5. MAPA
m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")

# Polígonos de Postgres
if not df_sec.empty:
    for _, row in df_sec.iterrows():
        folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

# Puntos con Popup EXACTO a la imagen
for _, r in df_mapa.iterrows():
    color_hex, etiqueta = get_color_logic(r['Nivel'], r['Consumo_diario'])
    
    # Construcción del HTML siguiendo image_a57246.png
    pop_html = f"""
    <div style="font-family: Arial; font-size: 12px; width: 450px; color: #333; line-height: 1.6;">
        <b>Cliente:</b> {r['ClientID_API']} - <b>Serie del medidor:</b> {r['Medidor']} - <b>Fecha de instalacion:</b> {r['Primer_instalacion']}<br>
        <b>Predio:</b> {r['Predio']}<br>
        <b>Nombre:</b> {r['Nombre']}<br>
        <b>Tarifa:</b> {r['Nivel']}<br>
        <b>Giro:</b> {r['Giro']}<br>
        <b>Dirección:</b> {r['Domicilio']} -<b>Colonia:</b> {r['Colonia']}<br>
        <b>Sector:</b> {r['Sector']} - <b>Nivel:</b> {r['Nivel']}<br>
        <b>Lectura:</b> {r['Lectura']} (m3) - <b>Ultima lectura:</b> {r['Fecha']}<br>
        <b>Consumo:</b> {r['Consumo_diario']:.2f} (m3) - <b>Consumo acumulado</b><br>
        <b>Tipo de comunicación:</b> {r['Metodoid_API']}<br><br>
        ANILLAS DE CONSUMO COLOR 3: <b>{etiqueta}</b>
    </div>
    """
    
    folium.CircleMarker(
        location=[r['Latitud'], r['Longitud']],
        radius=5, color=color_hex, fill=True, fill_opacity=0.9,
        popup=folium.Popup(pop_html, max_width=500)
    ).add_to(m)

st.title("Medidores inteligentes - Tablero de consumos")
folium_static(m, width=1200, height=600)
