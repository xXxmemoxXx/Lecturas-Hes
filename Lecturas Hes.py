import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN VISUAL
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

# 2. LÓGICA DE COLOR POR CONSUMO ACUMULADO
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50], 'COMERCIAL': [5, 10, 40, 60]}
    if v <= 0: return colors["CERO"]
    lim = config.get(nivel, config['DOMESTICO A'])
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# 3. CARGA DE DATOS
engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    st.write("### Periodo (FEB 2026)")
    f_inicio = st.date_input("Fecha de inicio", value=pd.Timestamp(2026, 2, 1))
    f_fin = st.date_input("Fecha de finalización", value=pd.Timestamp(2026, 2, 28))
    
    df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{f_inicio}' AND '{f_fin}'", engine)

# 4. PROCESAMIENTO (Usando nombres exactos de tu imagen)
df_mapa = df_hes.groupby('Medidor').agg({
    'Consumo_diario': 'sum',
    'Lectura': 'last',
    'Latitud': 'first',
    'Longitud': 'first',
    'Nivel': 'first',
    'ClientID_API': 'first',
    'Nombre': 'first',
    'Predio': 'first',
    'Domicilio': 'first',
    'Colonia': 'first',
    'Giro': 'first',
    'Sector': 'first',
    'Primer_instalacion': 'first',
    'Fecha': 'last'
}).reset_index()

# 5. DASHBOARD
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. de Consumo diario m3", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar los 4,664 puntos
    for _, r in df_mapa.iterrows():
        color_p = get_color_logic(r['Nivel'], r['Consumo_diario'])
        
        # Popup con los nombres de tu tabla
        pop_html = f"""<div style='font-size:11px; width:260px; color:black;'>
            <b>Cliente:</b> {r['ClientID_API']} - <b>Serie:</b> {r['Medidor']}<br>
            <b>F. Instalación:</b> {r['Primer_instalacion']}<br>
            <b>Nombre:</b> {r['Nombre']}<br>
            <b>Dirección:</b> {r['Domicilio']}, {r['Colonia']}<br>
            <b>Sector:</b> {r['Sector']} - <b>Nivel:</b> {r['Nivel']}<br>
            <b>Lectura:</b> {r['Lectura']} (m3) - {r['Fecha']}<br>
            <b>Consumo mes:</b> {r['Consumo_diario']:.2f} (m3)
        </div>"""
        
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.9,
            popup=folium.Popup(pop_html, max_width=300)
        ).add_to(m)
    
    folium_static(m, width=900, height=520)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(20), hide_index=True, height=400)
    
    # Gráfico corregido (Sin el error de px.colors.qualitative.Cyan)
    fig = px.pie(df_hes, names='Giro', hole=0.7, color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
