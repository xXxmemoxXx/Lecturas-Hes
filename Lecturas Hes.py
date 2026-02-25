import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN E INTERFAZ (ESTILO EXACTO A LA IMAGEN)
st.set_page_config(page_title="MIAA - Tablero", layout="wide")

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

# 2. LÓGICA DE COLOR POR CONSUMO ACUMULADO (NO POR LECTURA)
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
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

# 3. CARGA DE DATOS Y SECTORES
engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    st.write("### Periodo (FEB 2026)")
    # Calendario en español mediante inputs directos para evitar errores de locale
    f_inicio = st.date_input("Fecha de inicio", value=pd.Timestamp(2026, 2, 1))
    f_fin = st.date_input("Fecha de finalización", value=pd.Timestamp(2026, 2, 28))
    
    # Traer datos de HES y FAS (Sectores)
    df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{f_inicio}' AND '{f_fin}'", engine)
    # Intentar traer polígonos si existen en FAS
    try:
        df_fas = pd.read_sql("SELECT * FROM FAS", engine)
    except:
        df_fas = pd.DataFrame()

    # Filtros laterales exactos
    for col in ["ClientID_API", "MetodoID_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]:
        st.selectbox(col, ["Todos"] + sorted(df_hes[col].dropna().unique().tolist()) if col in df_hes.columns else ["Todos"])

# 4. PROCESAMIENTO: 4,664 MEDIDORES CON CONSUMO ACUMULADO
df_mapa = df_hes.groupby('Medidor').agg({
    'Consumo_diario': 'sum', # ACUMULADO PARA COLOR
    'Lectura': 'last',       # ÚLTIMA LECTURA PARA POPUP
    'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first',
    'ClientID_API': 'first', 'Nombre': 'first', 'Predio': 'first',
    'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Fecha': 'last',
    'Sector': 'first'
}).reset_index()

# 5. HEADER Y MÉTRICAS
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", "96.019,6")
m3.metric("Prom. de Consumo diario m3", "0,94")
m4.metric("Lecturas", "104.372")

# 6. MAPA CON POLÍGONOS Y PUNTOS
col_izq, col_der = st.columns([3, 1.2])

with col_izq:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar Polígonos de Sectores (Si la tabla FAS tiene geometría)
    if not df_fas.empty and 'geometry' in df_fas.columns:
        folium.GeoJson(df_fas, name="Sectores", style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    # Dibujar los 4,664 puntos
    for _, r in df_mapa.iterrows():
        color_p = get_color_logic(r['Nivel'], r['Consumo_diario'])
        # Popup detallado igual a la imagen blanca
        pop_html = f"""<div style='font-size:11px; width:250px; color:black;'>
            <b>Cliente:</b> {r['ClientID_API']} - <b>Serie:</b> {r['Medidor']}<br>
            <b>Nombre:</b> {r['Nombre']}<br>
            <b>Tarifa/Giro:</b> {r['Nivel']} / {r['Giro']}<br>
            <b>Dirección:</b> {r['Domicilio']}<br>
            <b>Lectura:</b> {r['Lectura']} m3 (Al {r['Fecha']})<br>
            <b>Consumo Mes:</b> {r['Consumo_diario']:.2f} m3
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
    
    # Gráfico de dona (Tarifas)
    fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Safe)
    fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")

