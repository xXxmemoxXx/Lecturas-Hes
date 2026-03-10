import streamlit as st
import pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# 2. CSS PARA DISEÑO DE FILTROS EXACTO A LAS IMÁGENES (CAJAS SÓLIDAS)
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #000000 !important; }
        
        /* Ocultar etiquetas externas */
        [data-testid="stWidgetLabel"] { display: none !important; }

        /* Diseño de caja sólida con borde blanco */
        div[data-baseweb="select"] {
            background-color: #000000 !important;
            border: 1px solid #ffffff !important;
            border-radius: 5px !important;
            min-height: 45px !important;
            margin-bottom: 12px !important;
        }

        /* Texto dentro de la caja (Blanco y centrado/izq) */
        div[data-baseweb="select"] div {
            color: white !important;
            font-weight: bold !important;
            font-size: 14px !important;
        }

        /* Flecha del selector en blanco */
        svg { fill: white !important; }

        /* Ajuste de espaciado en sidebar */
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 2px !important;
            padding-top: 2px !important;
        }
        
        /* Estilo de los chips seleccionados */
        div[data-testid="stMultiSelect"] span {
            background-color: #333333 !important;
            color: white !important;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

# 3. CONEXIONES A BASES DE DATOS
@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        user, host, db = creds["user"], creds["host"], creds["database"]
        pwd = urllib.parse.quote_plus(creds["password"])
        return create_engine(f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}")
    except Exception as e:
        st.error(f"Error MySQL: {e}"); return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except: return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if not conn: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn); conn.close(); return df
    except: return pd.DataFrame()

def reiniciar_tablero():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# 4. CARGA DE DATOS Y SIDEBAR
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    ahora = pd.Timestamp.now()
    inicio_mes = ahora.replace(day=1)
    
    st.write("**📅 Rango de Fechas**")
    fecha_rango = st.date_input("Periodo", value=(inicio_mes, ahora), max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")
    
    if len(fecha_rango) == 2:
        # Se cargan todos los campos para el popup completo
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # --- FILTROS CON PLACEHOLDER (DISEÑO SOLICITADO) ---
        st.markdown("<br>", unsafe_allow_html=True)
        # Campos exactos de tu tabla
        campos_filtros = ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        
        for col in campos_filtros:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                # El placeholder 'col' pone el nombre DENTRO de la caja blanca
                seleccion = st.multiselect(col, opciones, placeholder=col, key=f"f_{col}", label_visibility="collapsed")
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]
        
        st.divider()
        st.write("**Ranking Top 10 Consumo**")
        if not df_hes.empty:
            top10 = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_v = top10['Consumo_diario'].max() if not top10.empty else 1
            for _, row in top10.iterrows():
                rc1, rc2 = st.columns([1, 1])
                rc1.markdown(f"<span style='font-size:11px; color:#81D4FA;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / max_v) * 100
                rc2.markdown(f'<div style="width:{pct}%; background:#FF0000; height:6px; border-radius:2px; margin-top:5px;"></div>', unsafe_allow_html=True)
    else:
        st.stop()

# 5. PROCESAMIENTO PARA MAPA (Manteniendo todos los campos para el popup)
# Usamos agregación para no perder datos al agrupar por Medidor
mapeo_agg = {col: 'first' for col in df_hes.columns}
mapeo_agg.update({'Consumo_diario': 'sum', 'Lectura': 'last', 'Fecha': 'last'})
df_mapa = df_hes.groupby('Medidor').agg(mapeo_agg).reset_index()

# 6. DASHBOARD PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Medidores", f"{len(df_mapa):,}")
m2.metric("Consumo Total", f"{df_hes['Consumo_diario'].sum():,.1f} m3")
m3.metric("Promedio", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Sectores", f"{df_hes['Sector'].nunique()}" if 'Sector' in df_hes.columns else "0")

col_izq, col_der = st.columns([3, 1.2])

with col_izq:
    lat_c = df_mapa['Latitud'].mean() if not df_mapa.empty else 21.8853
    lon_c = df_mapa['Longitud'].mean() if not df_mapa.empty else -102.2916
    m = folium.Map(location=[lat_c, lon_c], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Dibujar sectores GeoJSON
    if not df_sec.empty:
        for _, s in df_sec.iterrows():
            folium.GeoJson(json.loads(s['geojson_data']),
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}
            ).add_to(m)

    # Marcadores con el POPUP GIGANTE ORIGINAL
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # TU ESTRUCTURA DE POPUP SIN ELIMINAR NADA
            pop_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; width: 300px; color: #333; line-height: 1.4;'>
                <h5 style='margin:0 0 8px 0; color: #007bff; border-bottom: 1px solid #ccc; padding-bottom: 3px;'>Detalle del Medidor</h5>
                <b>Cliente:</b> {r.get('ClienteID_API', 'N/A')} - <b>Serie:</b> {r['Medidor']}<br>
                <b>Fecha instalación:</b> {r.get('Primer_instalacion', 'N/A')}<br>
                <b>Predio:</b> {r.get('Predio', 'N/A')}<br>
                <b>Nombre:</b> {r.get('Nombre', 'N/A')}<br>
                <b>Tarifa:</b> {r.get('Nivel', 'N/A')}<br>
                <b>Giro:</b> {r.get('Giro', 'N/A')}<br>
                <b>Dirección:</b> {r.get('Domicilio', 'N/A')}<br>
                <b>Colonia:</b> {r.get('Colonia', 'N/A')}<br>
                <b>Sector:</b> {r.get('Sector', 'N/A')}<br>
                <b>Lectura:</b> {r.get('Lectura', 0):,.2f} (m3) - <b>Última:</b> {r.get('Fecha', 'N/A')}<br>
                <b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f} (m3) acumulado<br>
                <b>Tipo de comunicación:</b> {r.get('Metodoid_API', 'Lorawan')}<br><br>
                <div style='text-align: center; padding: 5px; background-color: {color_hex}22; border-radius: 4px; border: 1px solid {color_hex};'>
                    <b style='color: {color_hex};'>ANILLAS DE CONSUMO: {etiqueta}</b>
                </div>
            </div>"""
            
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']],
                radius=3, color=color_hex, fill=True, fill_opacity=0.9,
                popup=folium.Popup(pop_html, max_width=350)
            ).add_to(m)

    folium_static(m, width=950, height=600)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    if not df_hes.empty:
        st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(20).sort_values(by='Fecha', ascending=False), 
                     hide_index=True, use_container_width=True)

if st.button("♻️ Reiniciar Filtros", use_container_width=True):
    reiniciar_tablero()
