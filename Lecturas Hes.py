import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime

# CONFIGURACIÓN DE INTERFAZ NEGRA
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; font-weight: bold; }
    /* Ajuste para que los inputs se vean en fondo negro */
    div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {
        background-color: #1a1a1a !important; color: white !important; border: 1px solid #00d4ff !important;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

# LÓGICA DE COLORES POR RANGO (Simbología exacta de tu imagen)
def get_color_logic(nivel, volumen):
    v = float(volumen) if volumen else 0
    colors = {
        "REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", 
        "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"
    }
    config = {
        'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30],
        'DOMESTICO C': [8, 19, 37, 50], 'COMERCIAL': [5, 10, 40, 60],
        'ESTATAL PUBLICO': [17, 56, 143, 200], 'FEDERAL PUBLICO': [16, 68, 183, 200],
        'MUNICIPAL PUBLICO': [28, 72, 157, 200]
    }
    if v <= 0: return colors["CERO"]
    lim = config.get(nivel, config['DOMESTICO A'])
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# CARGA DE DATOS
def fetch_data(start, end):
    engine = get_mysql_engine()
    query = f"SELECT * FROM HES WHERE Fecha BETWEEN '{start}' AND '{end}'"
    df = pd.read_sql(query, engine)
    return df

# SIDEBAR CON RANGO DE FECHAS
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    date_range = st.date_input("Periodo de consulta", value=(datetime(2026, 2, 1), datetime(2026, 2, 28)))
    
    if len(date_range) == 2:
        df_full = fetch_data(date_range[0], date_range[1])
    else:
        st.stop()

# PROCESAMIENTO PARA EL MAPA: Un punto por cada medidor único (los 4,664)
# Agrupamos por Medidor para obtener su última posición y consumo acumulado en el mes
df_mapa = df_full.sort_values('Fecha').groupby('Medidor').last().reset_index()

# MÉTRICAS
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}") # Aquí aparecerán los 4,664 medidores
m2.metric("Consumo acumulado m3", f"{df_full['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{df_full['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_full):,}") # Aquí las 104,372 lecturas

# CUERPO PRINCIPAL
col_map, col_data = st.columns([3, 1.2])

with col_map:
    # Mapa centrado en Aguascalientes con fondo oscuro
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar los 4,664 medidores
    for _, r in df_mapa.iterrows():
        color_p = get_color_logic(r.get('Nivel'), r.get('Consumo_diario'))
        
        # Popup con campos de la base de datos (image_97cb01.png)
        html = f"""<div style='color:#333; font-size:11px; width:200px;'>
        <b>Cliente:</b> {r.get('ClientID_API')}<br>
        <b>Medidor:</b> {r.get('Medidor')}<br>
        <b>Nombre:</b> {r.get('Nombre')}<br>
        <b>Consumo:</b> {r.get('Consumo_diario')} m3<br>
        <b>Giro:</b> {r.get('Giro')}</div>"""
        
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.9,
            popup=folium.Popup(html, max_width=250)
        ).add_to(m)
    
    folium_static(m, width=900, height=550)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)

with col_data:
    st.write("**Detalle de Consumos**")
    st.dataframe(df_full[['Fecha', 'Medidor', 'Lectura', 'Consumo_diario']].head(50), hide_index=True)
    
    # Gráfica de dona por Giro
    fig = px.pie(df_full, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
