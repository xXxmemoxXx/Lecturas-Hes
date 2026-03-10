import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import time

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# CSS PARA DISEÑO DE FILTROS EXACTO A LA IMAGEN (TEXTO DENTRO DE CAJA)
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #000000 !important; }
        
        /* OCULTAR ETIQUETAS EXTERNAS TOTALMENTE */
        [data-testid="stWidgetLabel"] { display: none !important; }

        /* DISEÑO DE CAJA SÓLIDA TIPO BOTÓN */
        div[data-baseweb="select"] {
            background-color: #000000 !important;
            border: 1px solid #ffffff !important; /* Borde blanco nítido */
            border-radius: 5px !important;
            min-height: 45px !important;
            margin-bottom: 10px !important;
        }

        /* TEXTO DENTRO DE LA CAJA (BLANCO Y IZQUIERDA) */
        div[data-baseweb="select"] div {
            color: white !important;
            font-weight: bold !important;
            font-size: 14px !important;
        }

        /* ICONO DE FLECHA BLANCO */
        svg { fill: white !important; }

        /* ELIMINAR ESPACIADOS SOBRANTES */
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 2px !important;
            padding-top: 2px !important;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

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
    query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
    df = pd.read_sql(query, conn); conn.close(); return df

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

# CARGA INICIAL
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    # RANGO DE FECHAS
    ahora = pd.Timestamp.now()
    inicio_mes = ahora.replace(day=1)
    fecha_rango = st.date_input("Periodo", value=(inicio_mes, ahora), max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # --- FILTROS CON DISEÑO DE IMAGEN (PLACEHOLDER COMO ETIQUETA) ---
        filtros = ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        
        for col in filtros:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                # El truco: Usamos el nombre de la columna como placeholder para que aparezca DENTRO de la caja
                seleccion = st.multiselect(col, opciones, placeholder=col, key=f"f_{col}", label_visibility="collapsed")
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]
        
        st.divider()
        st.write("**Top 10 Consumo**")
        if not df_hes.empty:
            top10 = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            for _, row in top10.iterrows():
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"<span style='font-size:11px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / top10['Consumo_diario'].max()) * 100
                c2.markdown(f'<div style="width:{pct}%; background:#FF0000; height:6px; border-radius:2px;"></div>', unsafe_allow_html=True)
    else: st.stop()

# PROCESAMIENTO PARA MAPA
agg_mapa = {col: 'first' for col in df_hes.columns}
agg_mapa.update({'Consumo_diario': 'sum', 'Lectura': 'last', 'Fecha': 'last'})
df_mapa = df_hes.groupby('Medidor').agg(agg_mapa).reset_index()

# MAPA Y POPUP GIGANTE
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3 = st.columns(3)
m1.metric("Medidores", f"{len(df_mapa):,}")
m2.metric("Consumo Total", f"{df_hes['Consumo_diario'].sum():,.1f} m3")
m3.metric("Lecturas", f"{len(df_hes):,}")

m = folium.Map(location=[df_mapa['Latitud'].mean(), df_mapa['Longitud'].mean()], zoom_start=12, tiles="CartoDB dark_matter")

for _, r in df_mapa.iterrows():
    if pd.notnull(r['Latitud']):
        color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
        
        # --- TU POPUP EXACTO ---
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
            radius=2, color=color_hex, fill=True, fill_opacity=0.9,
            popup=folium.Popup(pop_html, max_width=350)
        ).add_to(m)

folium_static(m, width=1000, height=600)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    if not df_hes.empty:
        st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)

if st.button("🔄 Reiniciar Tablero", use_container_width=True): reiniciar_tablero()

