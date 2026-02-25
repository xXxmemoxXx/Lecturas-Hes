import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")
st.markdown("<style>.stApp { background-color: #000000 !important; color: white; }</style>", unsafe_allow_html=True)

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

@st.cache_data(ttl=3600)
def get_sectores_cached():
    try:
        pg_conn = get_postgres_conn()
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, pg_conn)
        pg_conn.close()
        return df
    except:
        return pd.DataFrame()

# 2. LÓGICA DE COLOR
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# 3. CARGA DE DATOS Y SIDEBAR
mysql_engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    
    # Botón de actualización corregido
    if st.button("🔄 Actualizar Sectores"):
        st.cache_data.clear()
        st.rerun()

    fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        filtros_sidebar = ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        filtros_activos = {}
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                seleccion = st.multiselect(f"{col}", options=opciones, key=f"f_{col}")
                filtros_activos[col] = seleccion
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]
    else:
        st.stop()

# Procesamiento de medidores
mapeo_columnas = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
                  'Nivel': 'first', 'ClientID_API': 'first', 'Nombre': 'first', 'Predio': 'first',
                  'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first',
                  'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}
df_mapa = df_hes.groupby('Medidor').agg({c: f for c, f in mapeo_columnas.items() if c in df_hes.columns}).reset_index()

# Zoom corregido
df_valid = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]
if not df_valid.empty and (filtros_activos.get("Colonia") or filtros_activos.get("Sector")):
    lat_centro, lon_centro, zoom_inicial = df_valid['Latitud'].mean(), df_valid['Longitud'].mean(), 14
else:
    lat_centro, lon_centro, zoom_inicial = 21.8853, -102.2916, 12

# 4. DASHBOARD (REESTRUCTURADO PARA MOSTRAR MÉTRICAS)
st.title("Medidores inteligentes - Tablero de consumos")

# Aquí van las métricas que desaparecieron
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    
    # Carga de sectores con resalte interactivo
    df_sec = get_sectores_cached()
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(
                json.loads(row['geojson_data']),
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1},
                highlight_function=lambda x: {'fillColor': '#ffff00', 'color': '#ffff00', 'weight': 3, 'fillOpacity': 0.4},
                tooltip=f"Sector: {row['sector']}"
            ).add_to(m)

    # Medidores con radio 2.5
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            pop_html = f"""<div style='font-family: Arial; font-size: 11px; width: 350px; color: #333;'>
                <b>Cliente:</b> {r.get('ClientID_API')} - <b>Serie:</b> {r.get('Medidor')}<br>
                <b>Nombre:</b> {r.get('Nombre')}<br><b>Tarifa:</b> {r.get('Nivel')}<br>
                <b>Dirección:</b> {r.get('Domicilio')} - <b>Sector:</b> {r.get('Sector')}<br>
                <b>Lectura:</b> {r.get('Lectura')} m3 - <b>Consumo:</b> {r.get('Consumo_diario', 0):.2f} m3
            </div>"""
            folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=2.5, color=color_hex, 
                                fill=True, fill_opacity=0.9, popup=folium.Popup(pop_html, max_width=400)).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)

st.button("Reset")
