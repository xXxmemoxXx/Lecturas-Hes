import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import json
import plotly.express as px
from datetime import datetime

# 1. CONFIGURACIÓN E INTERFAZ NEGRA
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; background-color: #000000; }
    /* Estilo para los filtros */
    div[data-baseweb="select"] > div { background-color: #1a1a1a; color: white; border: 1px solid #00d4ff; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIÓN A BASES DE DATOS
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

# 3. LÓGICA DE COLORES POR RANGOS (CASE SQL REPLICADO)
def get_color_logic(nivel, volumen):
    v = float(volumen) if volumen else 0
    # Paleta exacta de la imagen
    colors = {
        "REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", 
        "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"
    }
    
    config = {
        'DOMESTICO A': [5, 10, 15, 30],
        'DOMESTICO B': [6, 11, 20, 30],
        'DOMESTICO C': [8, 19, 37, 50],
        'COMERCIAL': [5, 10, 40, 60],
        'ESTATAL PUBLICO': [17, 56, 143, 200],
        'FEDERAL PUBLICO': [16, 68, 183, 200],
        'MUNICIPAL PUBLICO': [28, 72, 157, 200]
    }
    
    if v <= 0: return colors["CERO"]
    lim = config.get(nivel, config['DOMESTICO A'])
    
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# 4. CARGA DE DATOS (FILTRADO POR FECHA DESDE EL MOTOR)
def fetch_data_by_date(start_date, end_date):
    engine = get_mysql_engine()
    # Eliminamos el LIMIT para traer todas las lecturas del periodo seleccionado
    query = f"""
        SELECT * FROM HES 
        WHERE Fecha BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
    """
    return pd.read_sql(query, engine)

# 5. SIDEBAR: CONTROL DE FECHAS (TIPO CALENDARIO DOBLE)
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    
    # Selector de periodo (Feb 2026 por defecto)
    try:
        date_input = st.date_input("Periodo de consulta", 
                                   value=(datetime(2026, 2, 1), datetime(2026, 2, 28)))
        start_f, end_f = date_input
    except:
        st.warning("Seleccione un rango de fecha inicio y fin")
        st.stop()

    # Carga masiva de datos según el periodo
    df_tel = fetch_data_by_date(start_f, end_f)
    
    # Filtros adicionales
    f_sector = st.selectbox("Sector", ["Todos"] + sorted(df_tel['Sector'].dropna().unique().tolist()))
    if f_sector != "Todos":
        df_tel = df_tel[df_tel['Sector'] == f_sector]

    st.markdown('<div style="background-color: #1a0000; border: 1px solid red; padding: 10px; border-radius: 5px;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    st.table(df_tel.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']])

# 6. MÉTRICAS SUPERIORES
st.title("Medidores inteligentes - Tablero de consumos")
c1, c2, c3, c4 = st.columns(4)
c1.metric("N° de medidores", f"{df_tel['Medidor'].nunique():,}")
c2.metric("Consumo acumulado m3", f"{df_tel['Consumo_diario'].sum():,.1f}")
c3.metric("Prom. Consumo diario m3", f"{df_tel['Consumo_diario'].mean():.2f}")
c4.metric("Lecturas", f"{len(df_tel):,}") # Aquí verás las >100,000 lecturas

# 7. MAPA Y DETALLES
col_map, col_data = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Para no saturar el navegador con 100k puntos simultáneos, 
    # mostramos los más recientes o una muestra significativa si es "Todos"
    map_df = df_tel.head(10000) if len(df_tel) > 10000 else df_tel
    
    for _, r in map_df.iterrows():
        color_p = get_color_logic(r['Nivel'], r['Consumo_diario'])
        
        # HTML del Popup según imagen blanca detallada
        html_popup = f"""
        <div style="font-family: sans-serif; font-size: 11px; width: 250px; color: #333;">
            <b>Cliente:</b> {r['ClientID_API']} - <b>Serie:</b> {r['Medidor']}<br>
            <b>Predio:</b> {r['Predio']}<br>
            <b>Nombre:</b> {r['Nombre']}<br>
            <b>Tarifa/Nivel:</b> {r['Nivel']}<br>
            <b>Giro:</b> {r['Giro']}<br>
            <b>Dirección:</b> {r['Domicilio']} - {r['Colonia']}<br>
            <b>Lectura:</b> {r['Lectura']} (m3)<br>
            <b>Consumo:</b> {r['Consumo_diario']} (m3)<br>
            <b>Comunicación:</b> Lorawan
        </div>
        """
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.8,
            popup=folium.Popup(html_popup, max_width=300)
        ).add_to(m)
    
    folium_static(m, width=900, height=550)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)

with col_data:
    st.write("**Consumo real**")
    st.dataframe(df_tel[['Fecha', 'Lectura', 'Consumo_diario']].head(20), hide_index=True)
    
    fig = px.pie(df_tel, names='Giro', hole=0.6, color_discrete_sequence=px.colors.sequential.Teal_r)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
