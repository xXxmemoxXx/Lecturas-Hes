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

# 2. ESTILO CSS "CYBERPUNK"
st.markdown("""
    <style>
    .stApp { background-color: #000b16; color: #ffffff; }
    section[data-testid="stSidebar"] { background-color: #001529; border-right: 2px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; }
    h1, h2, h3 { color: #ffffff; text-shadow: 0 0 10px #00d4ff; border-bottom: 1px solid #00d4ff; }
    .alarm-box { background-color: #1a0000; border: 1px solid #ff0000; padding: 10px; border-radius: 5px; margin-top: 10px;}
    </style>
    """, unsafe_allow_html=True)

# 3. CONEXIONES
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 4. CARGA DE DATOS
@st.cache_data(ttl=600)
def fetch_data():
    engine = get_mysql_engine()
    # Traemos las columnas necesarias para el popup detallado
    query_mysql = "SELECT * FROM HES ORDER BY Fecha DESC LIMIT 5000"
    df_tel = pd.read_sql(query_mysql, engine)
    
    conn = get_postgres_conn()
    query_pg = """
        SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data 
        FROM "Sectorizacion"."Sectores_hidr";
    """
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

# 5. SIDEBAR Y FILTROS
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    st.date_input("Rango de fechas")
    
    # Filtros dinámicos
    f_sector = st.selectbox("Sector", ["Todos"] + sorted(list(df_tel['Sector'].dropna().unique())))
    f_colonia = st.selectbox("Colonia", ["Todos"] + sorted(list(df_tel['Colonia'].dropna().unique())))
    
    # Aplicar Filtro de Sector al DataFrame
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]
    if f_colonia != "Todos":
        df_tel = df_tel[df_tel['Colonia'] == f_colonia]

    st.markdown('<div class="alarm-box">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    top_df = df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.table(top_df)

# 6. MÉTRICAS (Header)
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. de Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_tel):,}")

# 7. MAPA Y PANELES
col_map, col_data = st.columns([3, 1.2])

with col_map:
    # Mapa Folium Dark
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar Sectores (Postgres)
    for _, row in df_sec.iterrows():
        folium.GeoJson(
            json.loads(row['geojson_data']),
            style_function=lambda x: {'fillColor': '#00FFFF', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.15}
        ).add_to(m)

    # Dibujar Medidores con Popup de la imagen blanca
    for _, r in df_tel.iterrows():
        color = "white" if r['Consumo_diario'] <= 0 else "orange" if r['Consumo_diario'] < 0.5 else "green" if r['Consumo_diario'] < 2.0 else "red"
        
        # Construcción del Popup según imagen detallada
        html_popup = f"""
        <div style="font-family: sans-serif; font-size: 12px; color: #333; width: 300px;">
            <b>Cliente:</b> {r['ClientID_API']} - <b>Serie:</b> {r['Medidor']}<br>
            <b>Fecha de instalacion:</b> {r['Primer_instalacion']}<br>
            <b>Predio:</b> {r['Predio']}<br>
            <b>Nombre:</b> {r['Nombre']}<br>
            <b>Tarifa/Nivel:</b> {r['Nivel']}<br>
            <b>Giro:</b> {r['Giro']}<br>
            <b>Dirección:</b> {r['Domicilio']} - <b>Colonia:</b> {r['Colonia']}<br>
            <b>Sector:</b> {r['Sector']} - <b>Nivel:</b> {r['Nivel']}<br>
            <b>Lectura:</b> {r['Lectura']} (m3)<br>
            <b>Consumo:</b> {r['Consumo_diario']} (m3)<br>
            <b>Tipo comunicación:</b> Lorawan
        </div>
        """
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color, fill=True, fill_opacity=0.8,
            popup=folium.Popup(html_popup, max_width=350)
        ).add_to(m)

    folium_static(m, width=900, height=600)

with col_data:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(15), height=350, hide_index=True)
    
    st.write("**Distribución por Giro**")
    # Corrección de colores de Plotly
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Cyan_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
