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

# CARGA DE SECTORES CON CACHÉ
@st.cache_data(ttl=3600)
def get_sectores_data():
    try:
        pg_conn = get_postgres_conn()
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, pg_conn)
        pg_conn.close()
        return df
    except Exception as e:
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

# 3. CARGA DE DATOS Y MENÚ LATERAL
mysql_engine = get_mysql_engine()
df_sec = get_sectores_data()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # --- FILTROS DINÁMICOS ---
        filtros_seleccionados = {}
        filtros_sidebar = ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                filtros_seleccionados[col] = st.multiselect(f"{col}", options=opciones, key=f"f_{col}")
                # Aplicación inmediata del filtro al DataFrame para que el mapa solo muestre lo seleccionado
                if filtros_seleccionados[col]:
                    df_hes = df_hes[df_hes[col].astype(str).isin(filtros_seleccionados[col])]

        st.markdown('<div style="background-color: #444; padding: 10px; border-radius: 5px; text-align: center; margin: 15px 0;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)

        st.write("**Ranking Top ... Consumo...**")
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            for _, row in ranking_data.iterrows():
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"<span style='color: #81D4FA; font-size: 13px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / max_c) * 100
                c2.markdown(f'<div style="display: flex; align-items: center; justify-content: flex-end;"><span style="font-size: 12px; margin-right: 5px;">{row["Consumo_diario"]:,.0f}</span><div style="width: 40px; background-color: #333; height: 8px; border-radius: 2px;"><div style="width: {pct}%; background-color: #FF0000; height: 8px; border-radius: 2px;"></div></div></div>', unsafe_allow_html=True)
    else:
        st.stop()

# --- PROCESAMIENTO PARA EL MAPA ---
mapeo_columnas = {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'ClientID_API': 'first', 'Nombre': 'first', 'Predio': 'first',
    'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first',
    'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'
}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()

# --- LÓGICA DE ZOOM DINÁMICO ---
if not df_mapa.empty and any(filtros_seleccionados.values()):
    centro_lat = df_mapa['Latitud'].mean()
    centro_lon = df_mapa['Longitud'].mean()
    zoom_val = 15
else:
    centro_lat, centro_lon = 21.8853, -102.2916
    zoom_val = 12

# 4. DASHBOARD PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")

m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_mapa.shape[0]:,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[centro_lat, centro_lon], zoom_start=zoom_val, tiles="CartoDB dark_matter
