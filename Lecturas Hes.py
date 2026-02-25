import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN Y ESTILO
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")
st.markdown("<style>.stApp { background-color: #000000 !important; color: white; }</style>", unsafe_allow_html=True)

# 2. CONEXIONES (AMBAS BASES DE DATOS)
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

def get_postgres_conn():
    # Conexión exacta a tu Postgres de QGIS
    return psycopg2.connect(
        user='map_tecnica', 
        password='M144.Tec', 
        host='ti.miaa.mx', 
        database='qgis', 
        port='5432'
    )

# 3. LÓGICA DE COLOR (CONSUMO ACUMULADO)
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    if v <= 0: return colors["CERO"]
    lim = config.get(str(nivel).upper(), [5, 10, 15, 30])
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# 4. CARGA DE DATOS
mysql_engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    fecha_rango = st.date_input("Periodo (FEB 2026)", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        # Datos de Medidores (MySQL)
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Datos de Sectores (Postgres) - Transformamos la geom a 4326 para Folium
        try:
            pg_conn = get_postgres_conn()
            query_pg = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
            df_sec = pd.read_sql(query_pg, pg_conn)
            pg_conn.close()
        except Exception as e:
            st.error(f"Error cargando polígonos de Postgres: {e}")
            df_sec = pd.DataFrame()
    else:
        st.stop()

# Procesamiento dinámico de columnas para evitar KeyError
mapeo = {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'ClientID_API': 'first', 'Nombre': 'first', 'Domicilio': 'first',
    'Colonia': 'first', 'Sector': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'
}
agg_final = {col: func for col, func in mapeo.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_final).reset_index()

# 5. DASHBOARD
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_real = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # --- DIBUJAR POLÍGONOS DE POSTGRES ---
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            geojson_dict = json.loads(row['geojson_data'])
            folium.GeoJson(
                geojson_dict,
                name=f"Sector {row['sector']}",
                style_function=lambda x: {
                    'fillColor': '#00d4ff',
                    'color': '#00d4ff',
                    'weight': 1.5,
                    'fillOpacity': 0.1
                },
                tooltip=f"Sector: {row['sector']}"
            ).add_to(m)

    # --- DIBUJAR PUNTOS DE MEDIDORES ---
    for _, r in df_mapa.iterrows():
        color_p = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
        pop_html = f"""<div style='font-size:11px; width:250px; color:black;'>
            <b>Cliente:</b> {r.get('ClientID_API')}<br>
            <b>Medidor:</b> {r.get('Medidor')}<br>
            <b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Consumo Mes:</b> {r.get('Consumo_diario', 0):.2f} m3
        </div>"""
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.9,
            popup=folium.Popup(pop_html, max_width=300)
        ).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_real:
    st.write("🟢 **Consumo real**")
    cols_der = [c for c in ['Fecha', 'Lectura', 'Consumo_diario'] if c in df_hes.columns]
    st.dataframe(df_hes[cols_der].tail(15), hide_index=True, height=450)
    
    if 'Nivel' in df_hes.columns:
        fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Safe)
        fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
        st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
