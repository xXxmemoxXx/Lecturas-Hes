import streamlit as st
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine
import urllib.parse
import folium
from streamlit_folium import folium_static
import plotly.express as px

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

# CSS para el look oscuro de la imagen
st.markdown("""
    <style>
    .stApp { background-color: #000b16; color: #ffffff; }
    section[data-testid="stSidebar"] { background-color: #001529; border-right: 2px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; }
    h1, h2, h3 { color: #ffffff; text-shadow: 0 0 10px #00d4ff; border-bottom: 1px solid #00d4ff; }
    .alarm-box { background-color: #1a0000; border: 1px solid #ff0000; padding: 10px; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIONES
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 3. CARGA DE DATOS
@st.cache_data(ttl=600)
def fetch_data():
    engine = get_mysql_engine()
    # Usamos los nombres exactos de tu tabla HES de la imagen
    df_tel = pd.read_sql("SELECT * FROM HES ORDER BY Fecha DESC LIMIT 5000", engine)
    
    conn = get_postgres_conn()
    df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', conn)
    conn.close()
    return df_tel, df_sec

df_tel, df_sec = fetch_data()

# 4. SIDEBAR Y FILTROS
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    st.date_input("Rango de fechas")
    
    # Filtros con manejo de errores para columnas inexistentes
    sectores_lista = sorted(df_tel['Sector'].dropna().unique()) if 'Sector' in df_tel.columns else []
    f_sector = st.selectbox("Sector", ["Todos"] + sectores_lista)
    
    colonias_lista = sorted(df_tel['Colonia'].dropna().unique()) if 'Colonia' in df_tel.columns else []
    f_colonia = st.selectbox("Colonia", ["Todos"] + colonias_lista)
    
    # Aplicar Filtros
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]
    if f_colonia != "Todos":
        df_tel = df_tel[df_tel['Colonia'] == f_colonia]

    st.markdown('<div class="alarm-box">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    if 'Consumo_diario' in df_tel.columns:
        st.table(df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']])

# 5. HEADER MÉTRICAS
st.title("Medidores inteligentes - Tablero de consumos")
c1, c2, c3, c4 = st.columns(4)
c1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
c2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
c3.metric("Prom. Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
c4.metric("Lecturas", f"{len(df_tel):,}")

# 6. MAPA Y DETALLES
col_map, col_data = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar Sectores (Postgres)
    for _, row in df_sec.iterrows():
        folium.GeoJson(json.loads(row['geojson_data']),
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    # Dibujar Puntos con POPUP EXACTO (Imagen 8)
    for _, r in df_tel.iterrows():
        # Lógica de colores de la imagen 1
        cons = r.get('Consumo_diario', 0)
        color = "white" if cons <= 0 else "orange" if cons < 0.5 else "green" if cons < 2.0 else "red"
        
        # HTML del Popup basado fielmente en image_9a0619.png
        html = f"""
        <div style="font-family: Arial; font-size: 11px; width: 280px;">
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
        folium.CircleMarker([r['Latitud'], r['Longitud']], radius=4, color=color, fill=True, popup=folium.Popup(html, max_width=300)).add_to(m)
    folium_static(m, width=900, height=550)

with col_data:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), hide_index=True)
    
    # Corrección error Plotly (Cian no existe como nombre directo en cualitativos)
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
