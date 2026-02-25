import streamlit as st
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine
import urllib.parse
import folium
from streamlit_folium import folium_static
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

# --- ESTILO CSS "CYBERPUNK" (Réplica exacta del diseño) ---
st.markdown("""
    <style>
    .stApp { background-color: #000b16; color: #ffffff; }
    section[data-testid="stSidebar"] { background-color: #001529; border-right: 2px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    .css-1r6slb0 { border: 1px solid #00d4ff; border-radius: 5px; padding: 10px; background: rgba(0,212,255,0.05); }
    .stDataFrame { border: 1px solid #00d4ff; }
    h1, h2, h3 { color: #ffffff; text-shadow: 0 0 10px #00d4ff; border-bottom: 1px solid #00d4ff; }
    /* Estilo para las alarmas en rojo */
    .alarm-box { background-color: #1a0000; border: 1px solid #ff0000; padding: 10px; border-radius: 5px; }
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
    # 1. Telemetría MySQL
    engine = get_mysql_engine()
    query_mysql = "SELECT * FROM HES ORDER BY Fecha DESC LIMIT 2000"
    df_telemetry = pd.read_sql(query_mysql, engine)
    
    # 2. Sectores PostgreSQL
    conn = get_postgres_conn()
    query_pg = """
        SELECT sector, "Pozos_Sector", "Poblacion", "Vol_Prod", "Superficie", "Long_Red", "U_Domesticos",
               ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
        FROM "Sectorizacion"."Sectores_hidr";
    """
    df_sectors = pd.read_sql(query_pg, conn)
    conn.close()
    return df_telemetry, df_sectors

# --- PROCESAMIENTO ---
try:
    df_tel, df_sec = fetch_data()
    df_tel['Latitud'] = pd.to_numeric(df_tel['Latitud'], errors='coerce')
    df_tel['Longitud'] = pd.to_numeric(df_tel['Longitud'], errors='coerce')
    df_tel = df_tel.dropna(subset=['Latitud', 'Longitud'])
except Exception as e:
    st.error(f"Error cargando bases de datos: {e}")
    st.stop()

# --- INTERFAZ IZQUIERDA (SIDEBAR) ---
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    st.date_input("Rango de fechas")
    filtros = ["ClientID_API", "MetodoID_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
    for f in filtros:
        st.selectbox(f, ["Todos"] + sorted(list(df_tel[f].unique() if f in df_tel else [])))
    
    st.markdown('<div class="alarm-box">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    st.write("**Ranking Top Consumo**")
    top_df = df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.table(top_df)

# --- CUERPO PRINCIPAL ---
st.title("Medidores inteligentes - Tablero de consumos")

# Fila de Métricas (Header)
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. de Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_tel):,}")

# --- MAPA Y TABLAS LATERALES ---
col_map, col_data = st.columns([3, 1.2])

with col_map:
    # Crear Mapa Folium con estilo oscuro
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Capa de Sectores (Postgres)
    for _, row in df_sec.iterrows():
        geojson_data = json.loads(row['geojson_data'])
        folium.GeoJson(
            geojson_data,
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.2},
            tooltip=f"Sector: {row['sector']}"
        ).add_to(m)

    # Capa de Medidores (MySQL)
    for _, row in df_tel.iterrows():
        # Lógica de colores según imagen
        c = row['Consumo_diario']
        dot_color = "white" if c <= 0 else "orange" if c < 0.5 else "green" if c < 2.0 else "red"
        
        folium.CircleMarker(
            location=[row['Latitud'], row['Longitud']],
            radius=3, color=dot_color, fill=True, fill_opacity=0.8,
            popup=f"Medidor: {row['Medidor']}<br>Consumo: {c} m3"
        ).add_to(m)

    folium_static(m, width=900, height=600)
    st.markdown("<div style='text-align: center; font-size: 10px;'>🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔵 NULL</div>", unsafe_allow_html=True)

with col_data:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), height=400, hide_index=True)
    
    # Gráfica de Dona (Esquina inferior derecha)
    st.write("**Distribución por Giro**")
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.qualitative.Cyan)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

# Botones Inferiores
b1, b2, b3 = st.columns([2, 1, 1])
with b2: st.button("Informe Ranking", use_container_width=True)
with b3: st.button("Reset", use_container_width=True)
