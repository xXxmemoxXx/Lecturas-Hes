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

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

# --- ESTILO CSS PARA FONDO NEGRO TOTAL ---
st.markdown("""
    <style>
    /* Fondo principal negro */
    .stApp { background-color: #000000; color: #ffffff; }
    /* Sidebar oscuro */
    section[data-testid="stSidebar"] { background-color: #000b16; border-right: 1px solid #00d4ff; }
    /* Métricas con estilo neón */
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    /* Ajuste de tablas y widgets */
    .stDataFrame { border: 1px solid #00d4ff; }
    div[data-baseweb="select"] > div { background-color: #1a1a1a; color: white; }
    </style>
    """, unsafe_allow_html=True)

# --- CONEXIONES ---
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# --- CARGA DE DATOS ---
@st.cache_data(ttl=600)
def fetch_data():
    engine = get_mysql_engine()
    # Traemos los datos de telemetría de MySQL
    df_tel = pd.read_sql("SELECT * FROM HES ORDER BY Fecha DESC LIMIT 5000", engine)
    
    # Traemos los polígonos de sectores de Postgres
    conn = get_postgres_conn()
    query_pg = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
    df_sec = pd.read_sql(query_pg, conn)
    conn.close()
    return df_tel, df_sec

try:
    df_tel, df_sec = fetch_data()
    df_tel['Latitud'] = pd.to_numeric(df_tel['Latitud'], errors='coerce')
    df_tel['Longitud'] = pd.to_numeric(df_tel['Longitud'], errors='coerce')
    df_tel = df_tel.dropna(subset=['Latitud', 'Longitud'])
except Exception as e:
    st.error(f"Error en bases de datos: {e}")
    st.stop()

# --- SIDEBAR: FILTROS Y CONTROL DE FECHAS ---
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    
    # CONTROL DE FECHAS (TIPO RANGO COMO EN LA IMAGEN)
    today = datetime.now()
    date_range = st.date_input(
        "Seleccione periodo (Fecha inicio - fin)",
        value=(datetime(2026, 2, 1), datetime(2026, 2, 28)),
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2030, 12, 31)
    )

    # FILTROS DINÁMICOS
    f_sector = st.selectbox("Sector", ["Todos"] + sorted(list(df_tel['Sector'].dropna().unique())))
    
    # Aplicar filtros
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]
    
    st.markdown('<div style="background-color: #1a0000; border: 1px solid red; padding: 10px; border-radius: 5px;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    st.write("**Ranking Top Consumo**")
    st.table(df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']])

# --- PANTALLA PRINCIPAL ---
st.title("Medidores inteligentes - Tablero de consumos")

# MÉTRICAS SUPERIORES
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_tel):,}")

# CUERPO DEL MAPA
col_map, col_info = st.columns([3, 1.2])

with col_map:
    # Mapa base oscuro
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar Polígonos de Sectores (Postgres)
    for _, row in df_sec.iterrows():
        folium.GeoJson(
            json.loads(row['geojson_data']),
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}
        ).add_to(m)

    # Dibujar Puntos con POPUP DETALLADO (Imagen Blanca)
    for _, r in df_tel.iterrows():
        cons = r.get('Consumo_diario', 0)
        color = "white" if cons <= 0 else "orange" if cons < 0.5 else "green" if cons < 2.0 else "red"
        
        # HTML del Popup según imagen detallada solicitada
        html_content = f"""
        <div style="font-family: Arial; font-size: 11px; width: 280px; color: #333;">
            <b>Cliente:</b> {r.get('ClientID_API', 'N/A')} - <b>Serie:</b> {r.get('Medidor', 'N/A')}<br>
            <b>Fecha de instalacion:</b> {r.get('Primer_instalacion', 'N/A')}<br>
            <b>Predio:</b> {r.get('Predio', 'N/A')}<br>
            <b>Nombre:</b> {r.get('Nombre', 'N/A')}<br>
            <b>Tarifa:</b> {r.get('Nivel', 'N/A')}<br>
            <b>Giro:</b> {r.get('Giro', 'N/A')}<br>
            <b>Dirección:</b> {r.get('Domicilio', 'N/A')} - <b>Colonia:</b> {r.get('Colonia', 'N/A')}<br>
            <b>Sector:</b> {r.get('Sector', 'N/A')} - <b>Nivel:</b> {r.get('Nivel', 'N/A')}<br>
            <b>Lectura:</b> {r.get('Lectura', 0)} (m3)<br>
            <b>Consumo:</b> {cons} (m3) - <b>Consumo acumulado</b><br>
            <b>Tipo comunicación:</b> Lorawan
        </div>
        """
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color, fill=True,
            popup=folium.Popup(html_content, max_width=300)
        ).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_info:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), hide_index=True)
    
    # GRÁFICA DE DONA (Corrección AttributeError)
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
