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
    /* Fondo negro global */
    .stApp { background-color: #000000; color: #ffffff; }
    /* Sidebar oscuro con borde neón */
    section[data-testid="stSidebar"] { background-color: #000b16; border-right: 1px solid #00d4ff; }
    /* Estilo de métricas */
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    /* Tablas con bordes cian */
    .stDataFrame { border: 1px solid #00d4ff; background-color: #000000; }
    /* Botones y dropdowns */
    div[data-baseweb="select"] > div { background-color: #1a1a1a; color: white; border: 1px solid #00d4ff; }
    button { background-color: #1a1a1a !important; color: #00d4ff !important; border: 1px solid #00d4ff !important; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIONES
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 3. LÓGICA DE COLORES SEGÚN RANGOS POR NIVEL
def get_status_color(nivel, volumen):
    v = float(volumen) if volumen else 0
    # Definición de rangos según el CASE facilitado
    config = {
        'DOMESTICO A': [(5, "BAJO", "orange"), (10, "REGULAR", "yellow"), (15, "NORMAL", "green"), (30, "ALTO", "darkred")],
        'DOMESTICO B': [(6, "BAJO", "orange"), (11, "REGULAR", "yellow"), (20, "NORMAL", "green"), (30, "ALTO", "darkred")],
        'DOMESTICO C': [(8, "BAJO", "orange"), (19, "REGULAR", "yellow"), (37, "NORMAL", "green"), (50, "ALTO", "darkred")],
        'COMERCIAL': [(5, "BAJO", "orange"), (10, "REGULAR", "yellow"), (40, "NORMAL", "green"), (60, "ALTO", "darkred")],
        'ESTATAL PUBLICO': [(17, "BAJO", "orange"), (56, "REGULAR", "yellow"), (143, "NORMAL", "green"), (200, "ALTO", "darkred")],
        'FEDERAL PUBLICO': [(16, "BAJO", "orange"), (68, "REGULAR", "yellow"), (183, "NORMAL", "green"), (200, "ALTO", "darkred")],
        'MUNICIPAL PUBLICO': [(28, "BAJO", "orange"), (72, "REGULAR", "yellow"), (157, "NORMAL", "green"), (200, "ALTO", "darkred")]
    }
    
    if v <= 0: return "white"  # CERO
    
    ranges = config.get(nivel, config['DOMESTICO A']) # Por defecto Domestico A si no coincide
    
    if v > ranges[-1][0]: return "red" # MUY ALTO
    for limit, label, color in ranges:
        if v <= limit: return color
    return "blue" # NULL/Otros

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
    st.error(f"Error: {e}")
    st.stop()

# 5. SIDEBAR: CONTROL DE FECHAS Y FILTROS
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    
    # Selector de fechas de la imagen
    date_range = st.date_input("Periodo de consulta", 
                               value=(datetime(2026, 2, 1), datetime(2026, 2, 28)))

    # Filtros
    f_sector = st.selectbox("Sector", ["Todos"] + sorted(list(df_tel['Sector'].dropna().unique())))
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]
    
    st.markdown('<div style="background-color: #1a0000; border: 1px solid red; padding: 10px; border-radius: 5px; color: white;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    st.table(df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']])

# 6. HEADER MÉTRICAS
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_tel):,}")

# 7. CUERPO (MAPA Y TABLA)
col_map, col_data = st.columns([3, 1.2])

with col_map:
    # Mapa base Dark
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Capa Sectores
    for _, row in df_sec.iterrows():
        folium.GeoJson(json.loads(row['geojson_data']),
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    # Capa Puntos con la nueva lógica de rangos
    for _, r in df_tel.iterrows():
        punto_color = get_status_color(r.get('Nivel'), r.get('Consumo_diario'))
        
        # Popup detallado imagen blanca
        html = f"""<div style="font-family: Arial; font-size: 11px; width: 280px;">
            <b>Cliente:</b> {r.get('ClientID_API')} - <b>Serie:</b> {r.get('Medidor')}<br>
            <b>Predio:</b> {r.get('Predio')}<br><b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Tarifa:</b> {r.get('Nivel')}<br><b>Giro:</b> {r.get('Giro')}<br>
            <b>Dirección:</b> {r.get('Domicilio')} - <b>Colonia:</b> {r.get('Colonia')}<br>
            <b>Sector:</b> {r.get('Sector')}<br><b>Lectura:</b> {r.get('Lectura')} (m3)<br>
            <b>Consumo:</b> {r.get('Consumo_diario')} (m3)<br><b>Comunicación:</b> Lorawan</div>"""
            
        folium.CircleMarker([r['Latitud'], r['Longitud']], radius=4, color=punto_color, fill=True, 
                            popup=folium.Popup(html, max_width=300)).add_to(m)
    
    folium_static(m, width=900, height=550)
    st.markdown("<p style='text-align: center; font-size: 12px;'>🟢 NORMAL | 🟡 REGULAR | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO</p>", unsafe_allow_html=True)

with col_data:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), hide_index=True)
    
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
