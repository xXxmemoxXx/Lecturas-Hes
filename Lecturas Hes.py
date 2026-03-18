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
import io
import base64
import matplotlib.pyplot as plt

# 1. CONFIGURACIÓN
st.set_page_config(
    page_title="MIAA - Tablero de Consumos",
    page_icon="https://www.miaa.mx/favicon.ico", 
    layout="wide"  
)

# ESTILO CSS (Tu diseño original preservado)
st.markdown("""
    <style>
        .titulo-superior {
            position: fixed; top: 15px; left: 50%; transform: translateX(-50%);
            z-index: 9999999; color: white; font-size: 1.2rem; font-weight: bold;
            pointer-events: none; white-space: nowrap;
        }
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        iframe[title="streamlit_folium.folium_static"] {
            border: 3px solid #444444 !important; border-radius: 10px;
            box-shadow: 0px 4px 15px rgba(0, 0, 0, 0.5);
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/c45d926ef0e34215c237cd3c7f71f7b97bf9a784/LogoMIAA-BpcVaQaq.svg"

@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        user, host, db = creds["user"], creds["host"], creds["database"]
        pwd = urllib.parse.quote_plus(creds["password"])
        return create_engine(f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}")
    except: return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except: return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if not conn: return pd.DataFrame()
    df = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', conn)
    conn.close()
    return df

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    lim = config.get(str(nivel).upper(), [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# FUNCIÓN PARA GRÁFICA EN TOOLTIP (Sin fallos de JSON)
def get_graph_base64(df_medidor):
    if df_medidor.empty: return ""
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(4, 2))
    ax.plot(df_medidor['Fecha'], df_medidor['Consumo_diario'], color='#00d4ff', marker='o', markersize=3)
    ax.fill_between(df_medidor['Fecha'], df_medidor['Consumo_diario'], alpha=0.2, color='#00d4ff')
    ax.tick_params(axis='x', rotation=45, labelsize=7)
    ax.tick_params(axis='y', labelsize=7)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', transparent=True)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

# CARGA DE DATOS
engine = get_mysql_engine()
df_sec = get_sectores_cached()
ahora = pd.Timestamp.now()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear(); st.cache_resource.clear(); st.rerun()
    st.divider()

    with st.expander("📅 RANGO DE FECHAS", expanded=True):
        opcion_rango = st.selectbox("Rango", ["Este mes", "Última semana", "Mes pasado", "Personalizado"])
        dr = (ahora.replace(day=1), ahora)
        fecha_rango = st.date_input("Periodo", value=dr, max_value=ahora)

    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", engine)
        
        with st.expander("🔍 FILTROS", expanded=False):
            for col in ["Medidor", "Colonia", "Sector"]:
                opts = sorted([str(x) for x in df_hes[col].unique() if pd.notnull(x)])
                sel = st.multiselect(col, options=opts)
                if sel: df_hes = df_hes[df_hes[col].astype(str).isin(sel)]

        with st.expander("🏆 RANKING TOP 10", expanded=True):
            if not df_hes.empty:
                ranking = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
                max_v = ranking['Consumo_diario'].max()
                for _, row in ranking.iterrows():
                    m_id = str(row['Medidor']).split('.')[0]
                    c1, c2, c3 = st.columns([1.2, 0.7, 1.1])
                    c1.markdown(f"<p style='color:#81D4FA; font-weight:800; margin:0;'>{m_id}</p>", unsafe_allow_html=True)
                    c2.markdown(f"<p style='color:white; font-weight:800; text-align:right; margin:0;'>{row['Consumo_diario']:,.0f}</p>", unsafe_allow_html=True)
                    pct = (row['Consumo_diario']/max_v)*100
                    c3.markdown(f'<div style="background:#262626; height:12px; border-radius:4px; margin-top:5px;"><div style="width:{pct}%; background:#FF0000; height:12px; border-radius:4px;"></div></div>', unsafe_allow_html=True)
    else: st.stop()

# PROCESAMIENTO
df_mapa = df_hes.groupby('Medidor').agg({'Consumo_diario':'sum', 'Latitud':'first', 'Longitud':'first', 'Nivel':'first', 'Nombre':'first', 'Sector':'first', 'Fecha':'last'}).reset_index()
df_v = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]

st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 Medidores", f"{len(df_mapa):,}")
m2.metric("💧 Consumo total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[df_v['Latitud'].mean(), df_v['Longitud'].mean()] if not df_v.empty else [21.88, -102.29], zoom_start=14, tiles="CartoDB dark_matter")
    Fullscreen().add_to(m)
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)
    
    for _, r in df_v.iterrows():
        color, etiq = get_color_logic(r['Nivel'], r['Consumo_diario'])
        # Generamos la gráfica solo para este medidor
        b64_img = get_graph_base64(df_hes[df_hes['Medidor'] == r['Medidor']])
        
        popup_html = f"""<div style='font-family:Arial; width:260px; color:#333;'>
            <b style='color:#007bff'>Medidor: {r['Medidor']}</b><br>
            <b>Consumo:</b> {r['Consumo_diario']:.2f} m³<br>
            <div style='background:{color}22; border:1px solid {color}; padding:3px; margin:5px 0; text-align:center;'><b>{etiq}</b></div>
            <img src="data:image/png;base64,{b64_img}" width="250">
        </div>"""
        
        folium.CircleMarker([r['Latitud'], r['Longitud']], radius=4, color=color, fill=True, fill_opacity=0.8, popup=folium.Popup(popup_html)).add_to(m)

    folium_static(m, width=1000, height=650)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True)

# GRÁFICOS INFERIORES (RESTABLECIDOS)
st.divider()
if not df_hes.empty:
    df_d = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
    fig1 = px.bar(df_d, x='Fecha', y='Consumo_diario', text_auto=',.2f', title="Consumo Total por Día")
    fig1.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white")
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = px.bar(df_mapa.sort_values('Consumo_diario', ascending=False), x='Medidor', y='Consumo_diario', title="Consumo por Medidor")
    fig2.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white")
    fig2.update_xaxes(type='category')
    st.plotly_chart(fig2, use_container_width=True)
