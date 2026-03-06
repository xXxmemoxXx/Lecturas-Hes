import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# Estilo visual: Fondo negro y diseño del sidebar para etiquetas a la izquierda
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        
        /* Alineación de filtros en Sidebar */
        section[data-testid="stSidebar"] .stMultiSelect {
            display: flex;
            flex-direction: row;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
        }
        
        section[data-testid="stSidebar"] .stMultiSelect label {
            min-width: 100px;
            margin-bottom: 0 !important;
            font-size: 14px;
            font-weight: bold;
            text-align: right;
            color: #E0E0E0;
        }
        
        section[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] {
            flex-grow: 1;
        }
    </style>
""", unsafe_allow_html=True)

# URL RAW del logo en GitHub
URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

# --- CONEXIONES ---

@st.cache_resource
def get_mysql_engine():
    """Conexión MySQL con manejo de caracteres especiales."""
    try:
        creds = st.secrets["mysql"]
        user = creds["user"]
        pwd = urllib.parse.quote_plus(creds["password"])
        host = creds["host"]
        db = creds["database"]
        conn_str = f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}"
        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Error MySQL: {e}")
        return None

@st.cache_resource
def get_postgres_conn():
    """Conexión PostgreSQL para capas geográficas."""
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e:
        st.error(f"Error Postgres: {e}")
        return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    """Carga de sectores desde PostGIS."""
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

# --- UTILIDADES ---

def reiniciar_tablero():
    """Limpia caché y reinicia aplicación con animación."""
    st.cache_data.clear()
    st.cache_resource.clear()
    placeholder = st.empty()
    with placeholder.container():
        st.markdown("<br><br><br><h1 style='text-align: center; font-size: 100px;'>🍞</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='text-align: center; color: white;'>Tu aplicación está en el horno</h3>", unsafe_allow_html=True)
    time.sleep(1.5) 
    st.rerun()

def get_color_logic(nivel, consumo_mes):
    """Lógica de colores según el nivel de consumo."""
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"]
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# --- LÓGICA DE CARGA ---

mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    if st.button("♻️ Actualizar Datos", key="btn_refresh", use_container_width=True):
        reiniciar_tablero()
    
    st.divider()
    
    try:
        fecha_rango = st.date_input("Periodo", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    except:
        st.stop()
    
    if len(fecha_rango) == 2:
        # Carga inicial de datos desde MySQL
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros con títulos internos (Placeholders)
        filtros_cfg = [
            ("Metodoid_API", "Método"), ("Medidor", "Medidor"), ("Predio", "Predio"), 
            ("Colonia", "Colonia"), ("Giro", "Giro"), ("Sector", "Sector")
        ]
        
        for col_db, label in filtros_cfg:
            if col_db in df_hes.columns:
                opciones = sorted(df_hes[col_db].unique().astype(str).tolist())
                # El uso de key=f"filter_{col_db}" previene el DuplicateElementKey
                seleccion = st.multiselect(label, options=opciones, key=f"filter_{col_db}")
                if seleccion:
                    df_hes = df_hes[df_hes[col_db].astype(str).isin(seleccion)]

        st.divider()
        
        # --- SECCIÓN DE RANKING (TU CÓDIGO INDEXADO CORRECTAMENTE) ---
        st.write("**Ranking Top 20 Consumo**")
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            
            for _, row in ranking_data.iterrows():
                rc1, rc2 = st.columns([1, 1])
                rc1.markdown(f"<span style='color: #81D4FA; font-size: 12px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                
                pct = (row['Consumo_diario'] / max_c) * 100
                rc2.markdown(f"""
                    <div style="display: flex; align-items: center; justify-content: flex-end;">
                        <span style="font-size: 11px; margin-right: 5px;">{row['Consumo_diario']:,.0f}</span>
                        <div style="width: 40px; background-color: #333; height: 8px; border-radius: 2px;">
                            <div style="width: {pct}%; background-color: #FF0000; height: 8px; border-radius: 2px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown('<div style="background-color: #444; padding: 10px; border-radius: 5px; text-align: center; margin: 15px 0;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    else:
        st.info("Seleccione un rango de fechas.")
        st.stop()

# 5. DASHBOARD
st.title("Medidores inteligentes - Tablero de consumos")

m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            geojson_obj = json.loads(row['geojson_data'])
            folium.GeoJson(
                geojson_obj,
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1},
                highlight_function=lambda x: {'fillColor': '#ffff00', 'color': '#ffff00', 'weight': 3, 'fillOpacity': 0.4},
                tooltip=folium.Tooltip(f"Sector: {row['sector']}", sticky=True)
            ).add_to(m)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            pop_html = f"<div style='font-family: Arial; font-size: 11px; width: 300px; color: #333;'><b>Medidor:</b> {r['Medidor']}<br><b>Consumo:</b> {r['Consumo_diario']:.2f} m3</div>"
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']],
                radius=3, color=color_hex, fill=True, fill_opacity=0.9,
                popup=folium.Popup(pop_html, max_width=350)
            ).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)

# Botón inferior
if st.button("Reset"):
    reiniciar_tablero()




