import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from folium.plugins import Fullscreen  
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# ESTILO CSS
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        
        /* Ajustes para los filtros alineados como en la imagen */
        [data-testid="stWidgetLabel"] p {
            font-size: 14px !important;
            color: white !important;
            margin-bottom: 0px !important;
            text-align: right;
        }
        
        .stMultiSelect {
            margin-bottom: 0px !important;
        }

        /* Estilo para la leyenda debajo del mapa */
        .map-legend {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 20px;
            padding: 15px;
            background-color: #000000;
            border-radius: 8px;
            margin-top: 10px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            font-size: 13px;
            font-weight: bold;
            color: white;
        }
        .legend-color {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        user = creds["user"]
        pwd = urllib.parse.quote_plus(creds["password"])
        host = creds["host"]
        db = creds["database"]
        conn_str = f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}"
        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Error configurando motor MySQL: {e}")
        return None

@st.cache_resource
def get_postgres_conn():
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e:
        st.error(f"Error conectando a Postgres: {e}")
        return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

def reiniciar_tablero():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

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

# CARGA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

ahora = pd.Timestamp.now()
inicio_mes_actual = ahora.replace(day=1)

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        reiniciar_tablero()
    st.divider()
    
    # --- FILTROS ALINEADOS COMO EN LA IMAGEN ---
    st.write("**Filtros de Medidores**")
    
    # Cargamos datos base para llenar los filtros
    df_base = pd.read_sql(f"SELECT * FROM HES WHERE Fecha >= '{inicio_mes_actual}'", mysql_engine)
    
    filtros_config = ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
    filtros_activos = {}
    
    for col in filtros_config:
        if col in df_base.columns:
            opciones = sorted(df_base[col].unique().astype(str).tolist())
            c1, c2 = st.columns([1, 1.5]) # Ajuste de proporción para nombre y selector
            with c1:
                st.markdown(f"<p style='margin-top:8px;'>{col}</p>", unsafe_allow_html=True)
            with c2:
                filtros_activos[col] = st.multiselect("", options=opciones, key=f"f_{col}", label_visibility="collapsed")

# Filtrado de datos
df_hes = df_base.copy()
for col, seleccion in filtros_activos.items():
    if seleccion:
        df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]

# PROCESAMIENTO
mapeo_columnas = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()
df_valid_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]

lat_centro, lon_centro = (df_valid_coords['Latitud'].mean(), df_valid_coords['Longitud'].mean()) if not df_valid_coords.empty else (21.8853, -102.2916)

# DASHBOARD
st.title("Medidores inteligentes - Tablero de consumos")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=13, tiles="CartoDB dark_matter")
    Fullscreen(position="topright").add_to(m)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # Popup con TODA la información restaurada
            pop_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; width: 300px; color: #333; line-height: 1.4; white-space: nowrap; display: inline-block;'>
                <h5 style='margin:0 0 8px 0; color: #007bff; border-bottom: 1px solid #ccc; padding-bottom: 3px;'>Detalle del Medidor</h5>
                <b>Cliente:</b> {r.get('ClienteID_API', 'N/A')} - <b>Serie:</b> {r['Medidor']}<br>
                <b>Fecha instalación:</b> {r.get('Primer_instalacion', 'N/A')}<br>
                <b>Predio:</b> {r.get('Predio', 'N/A')}<br>
                <b>Nombre:</b> {r.get('Nombre', 'N/A')}<br>
                <b>Tarifa:</b> {r.get('Nivel', 'N/A')}<br>
                <b>Giro:</b> {r.get('Giro', 'N/A')}<br>
                <b>Dirección:</b> {r.get('Domicilio', 'N/A')}<br>
                <b>Colonia:</b> {r.get('Colonia', 'N/A')}<br>
                <b>Sector:</b> {r.get('Sector', 'N/A')}<br>
                <b>Lectura:</b> {r.get('Lectura', 0):,.2f} (m3) - <b>Última:</b> {r.get('Fecha', 'N/A')}<br>
                <b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f} (m3) acumulado<br>
                <b>Tipo de comunicación:</b> {r.get('Metodoid_API', 'Lorawan')}<br><br>
                <div style='text-align: center; padding: 5px; background-color: {color_hex}22; border-radius: 4px; border: 1px solid {color_hex}; white-space: normal;'>
                    <b style='color: {color_hex};'>ANILLAS DE CONSUMO: {etiqueta}</b>
                </div>
            </div>
            """
            
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']],
                radius=4, color=color_hex, fill=True, fill_opacity=0.9,
                popup=folium.Popup(pop_html, max_width=350)
            ).add_to(m)
    
    folium_static(m, width=900, height=550)

    # --- LEYENDA (COMO EN LA IMAGEN) ---
    st.markdown("""
        <div class="map-legend">
            <div class="legend-item"><div class="legend-color" style="background-color: #00FF00;"></div>CONSUMO REGULAR</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #32CD32;"></div>CONSUMO NORMAL</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF8C00;"></div>CONSUMO BAJO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FFFFFF; border: 1px solid #555;"></div>CONSUMO CERO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF0000;"></div>CONSUMO MUY ALTO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #B22222;"></div>CONSUMO ALTO</div>
        </div>
    """, unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    if not df_hes.empty:
        st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)
