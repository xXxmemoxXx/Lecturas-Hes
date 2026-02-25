import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN VISUAL (FONDO NEGRO MIAA)
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")
st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    [data-testid="stMetricValue"] { font-size: 28px; color: #ffffff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

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

# 2. CARGA DE DATOS
mysql_engine = get_mysql_engine()
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    # Filtro de fecha igual a image_a4863e.png
    f_ini, f_fin = pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)
    fecha_rango = st.date_input("Periodo de consulta", value=(f_ini, f_fin))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        try:
            pg_conn = get_postgres_conn()
            df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', pg_conn)
            pg_conn.close()
        except: df_sec = pd.DataFrame()
    else: st.stop()

# 3. PROCESAMIENTO SEGURO (Mantiene todas las columnas para el popup)
agg_dict = {col: 'first' for col in df_hes.columns if col not in ['Medidor', 'Consumo_diario', 'Lectura', 'Fecha']}
agg_dict.update({'Consumo_diario': 'sum', 'Lectura': 'last', 'Fecha': 'last'})
df_mapa = df_hes.groupby('Medidor').agg(agg_dict).reset_index()

# 4. INTERFAZ PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")

# Métricas superiores como image_a4f20b.png y image_a4fd85.png
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), 
                           style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    for _, r in df_mapa.iterrows():
        color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
        
        # POPUP EXACTO A LA IMAGEN image_a57246.png
        pop_html = f"""
        <div style="font-family: Arial; font-size: 11px; width: 380px; color: #333; line-height: 1.5;">
            <b>Cliente:</b> {r.get('ClientID_API')} - <b>Serie del medidor:</b> {r.get('Medidor')} - <b>Fecha de instalacion:</b> {r.get('Primer_instalacion')}<br>
            <b>Predio:</b> {r.get('Predio')}<br>
            <b>Nombre:</b> {r.get('Nombre')}<br>
            <b>Tarifa:</b> {r.get('Nivel')}<br>
            <b>Giro:</b> {r.get('Giro')}<br>
            <b>Dirección:</b> {r.get('Domicilio')} - <b>Colonia:</b> {r.get('Colonia')}<br>
            <b>Sector:</b> {r.get('Sector')} - <b>Nivel:</b> {r.get('Nivel')}<br>
            <b>Lectura:</b> {r.get('Lectura')} (m3) - <b>Ultima lectura:</b> {r.get('Fecha')}<br>
            <b>Consumo:</b> {r.get('Consumo_diario', 0):.2f} (m3) - <b>Consumo acumulado</b><br>
            <b>Tipo de comunicación:</b> {r.get('Metodoid_API', 'Lorawan')}<br><br>
            <b>ANILLAS DE CONSUMO COLOR 3: <span style="color:{color_hex};">{etiqueta}</span></b>
        </div>
        """
        folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, 
                            fill_opacity=0.8, popup=folium.Popup(pop_html, max_width=400)).add_to(m)
    
    folium_static(m, width=900, height=550)
    # Simbología como image_a4e3fc.png
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO</p>", unsafe_allow_html=True)

# 5. APARTADO DE LA DERECHA (NUEVO)
with col_der:
    st.write("🟢 **Consumo real**")
    # Tabla de últimas lecturas
    resumen = df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(20)
    st.dataframe(resumen, hide_index=True, use_container_width=True)
    
    # Gráfico de dona por Tarifa/Nivel
    if 'Nivel' in df_hes.columns:
        fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
        st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
