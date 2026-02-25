import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px
import locale

# Intentar establecer calendario en español
try:
    locale.setlocale(locale.LC_TIME, "es_ES.utf8")
except:
    pass # Si el sistema no tiene el locale, continuará en inglés por defecto

# 1. CONFIGURACIÓN VISUAL (FONDO NEGRO Y ESTILO)
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #ffffff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; background-color: #000000; }
    /* Estilo para los selectores del sidebar */
    div[data-baseweb="select"] > div { background-color: #000000 !important; color: white !important; border: 1px solid #444 !important; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIÓN A BASE DE DATOS
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

# 3. LÓGICA DE COLORES SEGÚN IMAGEN DE SIMBOLOGÍA
def get_color(nivel, consumo):
    v = float(consumo) if consumo else 0
    # Colores exactos de la leyenda
    colors = {
        "REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", 
        "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"
    }
    # Rangos según Nivel (Basado en lógica previa del proyecto)
    config = {
        'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30],
        'DOMESTICO C': [8, 19, 37, 50], 'COMERCIAL': [5, 10, 40, 60]
    }
    if v <= 0: return colors["CERO"]
    lim = config.get(nivel, config['DOMESTICO A'])
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# 4. CARGA DE DATOS
engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    
    # CALENDARIO EN ESPAÑOL (Manejado por el navegador si el locale falla)
    st.write("### Periodo")
    fecha_rango = st.date_input("Seleccione rango", value=(pd.Timestamp(2026,2,1), pd.Timestamp(2026,2,28)))
    
    if len(fecha_rango) == 2:
        query = f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'"
        df = pd.read_sql(query, engine)
    else:
        st.stop()

    # FILTROS (Réplica de la izquierda)
    st.selectbox("ClientID_API", ["Todos"])
    st.selectbox("MetodoID_API", ["Todos"])
    st.selectbox("Medidor", ["Todos"])
    st.selectbox("Predio", ["Todos"])
    st.selectbox("Colonia", ["Todos"])
    st.selectbox("Giro", ["Todos"])
    st.selectbox("Sector", ["Todos"])
    
    st.error("⚠️ Informe alarmas")
    st.write("**Ranking Top Consumo**")
    top_10 = df.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.dataframe(top_10, hide_index=True)

# 5. HEADER DE MÉTRICAS (Exacto a la imagen)
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
# Datos sacados de tu imagen real
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", "96.019,6")
m3.metric("Prom. de Consumo diario m3", "0,94")
m4.metric("Lecturas", "104.372")

# 6. DISTRIBUCIÓN PRINCIPAL (MAPA Y PANEL DERECHO)
col_izq, col_der = st.columns([3, 1.2])

with col_izq:
    # MAPA
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Procesar 4,664 puntos únicos
    df_map = df.sort_values('Fecha').groupby('Medidor').last().reset_index()
    
    for _, r in df_map.iterrows():
        color_p = get_color(r.get('Nivel'), r.get('Consumo_diario'))
        # Popup exacto según image_9a0619.png
        popup_html = f"""<div style='font-size:11px; width:200px; color:#333;'>
            <b>Cliente:</b> {r.get('ClientID_API')}<br>
            <b>Serie:</b> {r.get('Medidor')}<br>
            <b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Lectura:</b> {r.get('Lectura')} (m3)
        </div>"""
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=250)
        ).add_to(m)
    
    folium_static(m, width=900, height=520)
    # LEYENDA (Exacta a image_a4e3fc.png)
    st.markdown("<p style='text-align: center; font-size:12px;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)
    
    col_b1, col_b2 = st.columns(2)
    col_b1.button("Informe Ranking", use_container_width=True)
    col_b2.button("Reset", use_container_width=True)

with col_der:
    # PANEL DERECHO: Consumo Real (Exacto a image_974c3a.jpg)
    st.write("🟢 **Consumo real**")
    # Tabla de lecturas recientes
    df_side = df[['Fecha', 'Lectura', 'Consumo_diario']].head(15).copy()
    df_side.columns = ['Fecha', 'Lectura', 'm3']
    st.dataframe(df_side, hide_index=True, height=450)
    
    # GRÁFICO DE DONA (Exacto a la imagen)
    # Mapeo de colores para las tarifas
    tarifas_colors = {
        'COMERCIAL': '#00d4ff', 'DOMESTICO A': '#ff00ff', 'DOMESTICO B': '#ff0080',
        'DOMESTICO C': '#00ff00', 'ESTATAL': '#ff8000'
    }
    fig = px.pie(df, names='Nivel', hole=0.7, 
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), 
                      paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)
    
    # Leyenda del gráfico (Manual para que quepa)
    st.caption("🔵 COMERCIAL | 🟣 DOMESTICO A | 🔴 DOMESTICO B | 🟢 DOMESTICO C")
