import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
from datetime import datetime

# 1. CONFIGURACIÓN DE PÁGINA Y ESTILO NEGRO TOTAL
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    /* Fondo negro global y texto blanco */
    .stApp { background-color: #000000 !important; color: #ffffff; }
    /* Sidebar oscuro con borde neón azul */
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    /* Métricas con estilo neón */
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    /* Tablas con estética de la imagen */
    .stDataFrame { border: 1px solid #00d4ff; background-color: #000000; }
    /* Estilo para los filtros y inputs */
    div[data-baseweb="select"] > div { background-color: #1a1a1a; color: white; border: 1px solid #444; }
    div[data-baseweb="input"] > div { background-color: #1a1a1a; color: white; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIONES
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 3. LÓGICA DE COLORES SEGÚN RANGOS Y NIVELES (CASE ORIGINAL)
def get_color_and_label(nivel, volumen):
    v = float(volumen) if volumen else 0
    
    # Definición de umbrales por Nivel
    config = {
        'DOMESTICO A': [5.00, 10.00, 15.00, 30.00],
        'DOMESTICO B': [6.00, 11.00, 20.00, 30.00],
        'DOMESTICO C': [8.00, 19.00, 37.00, 50.00],
        'COMERCIAL': [5.00, 10.00, 40.00, 60.00],
        'ESTATAL PUBLICO': [17.00, 56.00, 143.00, 200.00],
        'FEDERAL PUBLICO': [16.00, 68.00, 183.00, 200.00],
        'MUNICIPAL PUBLICO': [28.00, 72.00, 157.00, 200.00]
    }
    
    # Colores exactos de la imagen
    palette = {
        "CERO": "#FFFFFF",      # Blanco
        "BAJO": "#FF8C00",      # Naranja (DarkOrange)
        "REGULAR": "#7FFF00",   # Verde Lima (Chartreuse)
        "NORMAL": "#00FF00",    # Verde Brillante
        "ALTO": "#8B0000",      # Rojo Oscuro (DarkRed)
        "MUY ALTO": "#FF0000",  # Rojo Brillante
        "NULL": "#0000FF"       # Azul
    }

    if v <= 0: return palette["CERO"]
    
    limits = config.get(nivel, config['DOMESTICO A'])
    
    if v <= limits[0]: return palette["BAJO"]
    if v <= limits[1]: return palette["REGULAR"]
    if v <= limits[2]: return palette["NORMAL"]
    if v <= limits[3]: return palette["ALTO"]
    return palette["MUY ALTO"]

# 4. CARGA DE DATOS
@st.cache_data(ttl=600)
def fetch_data():
    engine = get_mysql_engine()
    df_tel = pd.read_sql("SELECT * FROM HES ORDER BY Fecha DESC LIMIT 5000", engine)
    
    conn = get_postgres_conn()
    df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', conn)
    conn.close()
    return df_tel, df_sec

try:
    df_tel, df_sec = fetch_data()
    df_tel['Latitud'] = pd.to_numeric(df_tel['Latitud'], errors='coerce')
    df_tel['Longitud'] = pd.to_numeric(df_tel['Longitud'], errors='coerce')
    df_tel = df_tel.dropna(subset=['Latitud', 'Longitud'])
except Exception as e:
    st.error(f"Error de conexión: {e}")
    st.stop()

# 5. SIDEBAR: FILTROS Y CALENDARIO
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    
    # Selector de rango de fechas (Fondo oscuro por CSS arriba)
    date_range = st.date_input("Periodo (Inicio - Fin)", 
                               value=(datetime(2026, 2, 1), datetime(2026, 2, 28)))

    # Filtros de búsqueda
    sectores = ["Todos"] + sorted(df_tel['Sector'].dropna().unique().tolist())
    f_sector = st.selectbox("Sector", sectores)
    
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]
    
    st.markdown('<div style="background-color: #1a0000; border: 1px solid red; padding: 10px; border-radius: 5px;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    st.table(df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']])

# 6. HEADER MÉTRICAS (Estilo imagen 1)
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_tel):,}")

# 7. MAPA Y PANEL DERECHO
col_map, col_info = st.columns([3, 1.2])

with col_map:
    # Mapa Folium con estilo Dark Matter
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Sectores (Polígonos)
    for _, row in df_sec.iterrows():
        folium.GeoJson(json.loads(row['geojson_data']),
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    # Medidores (Puntos con Popup detallado de imagen blanca)
    for _, r in df_tel.iterrows():
        punto_color = get_color_and_label(r.get('Nivel'), r.get('Consumo_diario'))
        
        # Estructura del Popup (Réplica exacta de imagen blanca)
        html = f"""
        <div style="font-family: Arial; font-size: 11px; width: 260px; color: #333;">
            <b>Cliente:</b> {r.get('ClientID_API')} - <b>Serie:</b> {r.get('Medidor')}<br>
            <b>Instalación:</b> {r.get('Primer_instalacion')}<br>
            <b>Predio:</b> {r.get('Predio')}<br>
            <b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Tarifa:</b> {r.get('Nivel')}<br>
            <b>Dirección:</b> {r.get('Domicilio')} - {r.get('Colonia')}<br>
            <b>Consumo Diario:</b> {r.get('Consumo_diario')} m3<br>
            <b>Lectura:</b> {r.get('Lectura')} m3
        </div>
        """
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=punto_color, fill=True, fill_opacity=0.9,
            popup=folium.Popup(html, max_width=300)
        ).add_to(m)
    
    folium_static(m, width=900, height=550)
    
    # Leyenda Inferior (Simbología)
    st.markdown("""
    <div style="text-align: center; font-size: 12px; margin-top: 10px;">
    <span style="color:#7FFF00">●</span> REGULAR | <span style="color:#00FF00">●</span> NORMAL | <span style="color:#FF8C00">●</span> BAJO | 
    <span style="color:#FFFFFF">●</span> CERO | <span style="color:#FF0000">●</span> MUY ALTO | <span style="color:#8B0000">●</span> ALTO | <span style="color:#0000FF">●</span> NULL
    </div>
    """, unsafe_allow_html=True)

with col_info:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), hide_index=True)
    
    # Gráfica de Dona (Giro)
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)', font=dict(color="white"))
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
