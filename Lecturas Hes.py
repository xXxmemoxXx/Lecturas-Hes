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
st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #ffffff; font-weight: bold; }
    div[data-baseweb="select"] > div { background-color: #1a1a1a !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    n = str(nivel).upper()
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# 2. CARGA DE DATOS Y PANEL IZQUIERDO
mysql_engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros Sidebar
        for col in ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]:
            if col in df_hes.columns:
                st.selectbox(col, ["Todos"] + sorted(df_hes[col].unique().tolist()))
        
        st.error("⚠️ Informe alarmas")
        st.write("**Ranking Top Consumo**")
        top_10 = df_hes.groupby('Medidor')['Consumo_diario'].sum().nlargest(10).reset_index()
        st.dataframe(top_10, hide_index=True)
        
        try:
            pg_conn = get_postgres_conn()
            df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', pg_conn)
            pg_conn.close()
        except: df_sec = pd.DataFrame()
    else: st.stop()

# 3. PROCESAMIENTO MAPA
agg_dict = {col: 'first' for col in df_hes.columns if col not in ['Medidor', 'Consumo_diario', 'Lectura', 'Fecha']}
agg_dict.update({'Consumo_diario': 'sum', 'Lectura': 'last', 'Fecha': 'last'})
df_mapa = df_hes.groupby('Medidor').agg(agg_dict).reset_index()

# 4. CUERPO PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), 
                           style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    for _, r in df_mapa.iterrows():
        color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
        pop_html = f"""
        <div style="font-family: Arial; font-size: 11px; width: 350px; color: #333;">
            <b>Cliente:</b> {r.get('ClientID_API')} - <b>Serie:</b> {r.get('Medidor')} - <b>Instalación:</b> {r.get('Primer_instalacion')}<br>
            <b>Predio:</b> {r.get('Predio')} - <b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Dirección:</b> {r.get('Domicilio')} - <b>Colonia:</b> {r.get('Colonia')}<br>
            <b>Lectura:</b> {r.get('Lectura')} m3 - <b>Última:</b> {r.get('Fecha')}<br>
            <b>Consumo Mes:</b> {r.get('Consumo_diario', 0):.2f} m3 - <b>Comunicación:</b> {r.get('Metodoid_API')}<br><br>
            <b>ANILLAS DE CONSUMO: {etiqueta}</b>
        </div>"""
        folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, 
                            fill_opacity=0.9, popup=folium.Popup(pop_html, max_width=400)).add_to(m)
    
    folium_static(m, width=900, height=550)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO</p>", unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)
    fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Safe)
    fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
