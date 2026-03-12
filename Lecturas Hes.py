import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import Fullscreen
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# ESTILO CSS (Exacto a tu diseño de fondo negro y bordes cian)
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #0c0c0c !important; border-right: 1px solid #00d4ff; }
        
        /* Contenedor de indicadores */
        .metric-row {
            display: flex;
            justify-content: space-around;
            background-color: #000;
            border: 2px solid #00d4ff;
            padding: 10px;
            margin-bottom: 10px;
        }
        .metric-box { text-align: center; border-right: 1px solid #444; flex: 1; }
        .metric-box:last-child { border-right: none; }
        .metric-label { font-size: 12px; color: #ccc; margin-bottom: 5px; }
        .metric-value { font-size: 20px; font-weight: bold; color: white; }
        .metric-icon { width: 30px; height: 30px; vertical-align: middle; margin-right: 8px; }

        /* Ajustes Sidebar */
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 0px !important; padding-top: 0px !important; margin-bottom: -5px !important;
        }
        .stMultiSelect { margin-bottom: 10px !important; }
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
    except: return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except: return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except: return pd.DataFrame()

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

mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    # BOTÓN DE ACTUALIZACIÓN / DESPERTAR
    if st.button("🔄 ACTUALIZAR / DESPERTAR", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    ahora = pd.Timestamp.now()
    fecha_rango = st.date_input("Periodo de consulta", value=(ahora.replace(day=1), ahora))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        for col in ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                sel = st.multiselect(col, opciones, key=f"s_{col}")
                if sel: df_hes = df_hes[df_hes[col].astype(str).isin(sel)]

        st.divider()
        st.write("**Ranking Top 10 Consumo**")
        if not df_hes.empty:
            ranking = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            for _, row in ranking.iterrows():
                c1, c2 = st.columns([1.5, 1])
                c1.markdown(f"<span style='color:#81D4FA; font-size:11px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / ranking['Consumo_diario'].max()) * 100
                c2.markdown(f'<div style="display:flex;align-items:center;justify-content:flex-end;"><span style="font-size:10px;margin-right:5px;">{row["Consumo_diario"]:,.0f}</span><div style="width:30px;background:#333;height:6px;"><div style="width:{pct}%;background:red;height:6px;"></div></div></div>', unsafe_allow_html=True)

# PROCESAMIENTO
agg_map = {col: func for col, func in {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first',
    'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first',
    'Primer_instalacion': 'first', 'Fecha': 'last', 'ClienteID_API': 'first'
}.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_map).reset_index()

# INTERFAZ PRINCIPAL
st.markdown(f'<h2 style="margin-top:-20px;">Medidores inteligentes - Tablero de consumos</h2>', unsafe_allow_html=True)

# INDICADORES SUPERIORES
st.markdown(f"""
    <div class="metric-row">
        <div class="metric-box"><p class="metric-label">N° de medidores</p><p class="metric-value">{len(df_mapa):,}</p></div>
        <div class="metric-box">
            <p class="metric-label">Consumo acumulado m3</p>
            <p class="metric-value"><img src="https://cdn-icons-png.flaticon.com/512/3105/3105807.png" class="metric-icon">{df_hes['Consumo_diario'].sum():,.2f}</p>
        </div>
        <div class="metric-box"><p class="metric-label">Prom. de Consumo diario m3</p><p class="metric-value">{df_hes['Consumo_diario'].mean():,.2f}</p></div>
        <div class="metric-box">
            <p class="metric-label">Lecturas</p>
            <p class="metric-value"><img src="https://cdn-icons-png.flaticon.com/512/2666/2666505.png" class="metric-icon">{len(df_hes):,}</p>
        </div>
    </div>
""", unsafe_allow_html=True)

c_left, c_right = st.columns([3, 1.2])

with c_left:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles=None)
    folium.TileLayer('CartoDB dark_matter', name="Mapa Negro", control=True).add_to(m)
    folium.TileLayer('OpenStreetMap', name="Mapa Estándar", control=True).add_to(m)
    folium.TileLayer(tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Satélite', control=True).add_to(m)
    
    Fullscreen().add_to(m)

    if not df_sec.empty:
        s_group = folium.FeatureGroup(name="Sectores Hidrométricos").add_to(m)
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(s_group)

    m_group = folium.FeatureGroup(name="Medidores").add_to(m)
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']):
            color, _ = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            pop = f"<b>Serie:</b> {r['Medidor']}<br><b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f}"
            folium.CircleMarker([r['Latitud'], r['Longitud']], radius=5, color=color, fill=True, fill_opacity=0.9, tooltip=pop).add_to(m_group)

    folium.LayerControl(collapsed=False).add_to(m)
    
    res = st_folium(m, width=900, height=500, key="main_map", returned_objects=["last_object_clicked"])

with c_right:
    med_sel = None
    if res and res.get("last_object_clicked"):
        lat, lon = res["last_object_clicked"]["lat"], res["last_object_clicked"]["lng"]
        match = df_mapa[(abs(df_mapa['Latitud'] - lat) < 0.0001) & (abs(df_mapa['Longitud'] - lon) < 0.0001)]
        if not match.empty: med_sel = match.iloc[0]['Medidor']

    st.markdown(f'<div style="background:#111;padding:5px;border:1px solid #00d4ff;font-size:18px;">🆔 {med_sel if med_sel else "Seleccione medidor"}</div>', unsafe_allow_html=True)
    
    if med_sel:
        df_v = df_hes[df_hes['Medidor'] == med_sel].sort_values('Fecha', ascending=False)
        st.dataframe(df_v[['Fecha', 'Lectura', 'Consumo_diario']], height=300, hide_index=True)
    else:
        st.dataframe(df_hes[['Medidor', 'Fecha', 'Lectura', 'Consumo_diario']].tail(10), height=300, hide_index=True)

    if not df_hes.empty and 'Giro' in df_hes.columns:
        fig = px.pie(df_hes, names='Giro', hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
