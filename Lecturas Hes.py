import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")
st.markdown("<style>.stApp { background-color: #000000 !important; color: white; }</style>", unsafe_allow_html=True)

@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# CARGA DE SECTORES CON CACHÉ PARA EVITAR QUE DESAPAREZCAN
@st.cache_data(ttl=3600)
def get_sectores_data():
    try:
        pg_conn = get_postgres_conn()
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, pg_conn)
        pg_conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

# 2. LÓGICA DE COLOR
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

# 3. CARGA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_data()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros Dinámicos
        filtros_seleccionados = {}
        filtros_sidebar = ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                filtros_seleccionados[col] = st.multiselect(f"{col}", options=opciones, key=f"f_{col}")
                if filtros_seleccionados[col]:
                    df_hes = df_hes[df_hes[col].astype(str).isin(filtros_seleccionados[col])]

        st.markdown('<div style="background-color:#444;padding:10px;border-radius:5px;text-align:center;margin:15px 0;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)

        st.write("**Ranking Top Consumo**")
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            for _, row in ranking_data.iterrows():
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"<span style='color:#81D4FA;font-size:13px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / max_c) * 100
                c2.markdown(f'<div style="display:flex;align-items:center;justify-content:flex-end;"><span style="font-size:12px;margin-right:5px;">{row["Consumo_diario"]:,.0f}</span><div style="width:40px;background-color:#333;height:8px;border-radius:2px;"><div style="width:{pct}%;background-color:#FF0000;height:8px;border-radius:2px;"></div></div></div>', unsafe_allow_html=True)
    else:
        st.stop()

# Procesamiento para el mapa
mapeo_columnas = {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'ClientID_API': 'first', 'Nombre': 'first', 'Predio': 'first',
    'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first',
    'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'
}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()

# LÓGICA DE ZOOM AUTOMÁTICO
if not df_mapa.empty and (filtros_seleccionados.get('Sector') or filtros_seleccionados.get('Colonia')):
    centro_mapa = [df_mapa['Latitud'].mean(), df_mapa['Longitud'].mean()]
    zoom_inicial = 14
else:
    centro_mapa = [21.8853, -102.2916]
    zoom_inicial = 12

# 4. DASHBOARD PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")

m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_mapa.shape[0]:,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=centro_mapa, zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            try:
                folium.GeoJson(
                    json.loads(row['geojson_data']),
                    style_function=lambda x: {'fillColor':'#00d4ff','color':'#00d4ff','weight':1.5,'fillOpacity':0.2},
                    tooltip=f"Sector: {row['sector']}"
                ).add_to(m)
            except: continue

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            pop_html = f'<div style="font-family:Arial;font-size:11px;width:300px;color:#333;"><b>Serie:</b> {r.get("Medidor")}<br><b>Consumo:</b> {r.get("Consumo_diario", 0):.2f} m3<br><b style="color:{color_hex};">ESTADO: {etiqueta}</b></div>'
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, fill_opacity=0.9,
                popup=folium.Popup(pop_html, max_width=350)
            ).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)
    if 'Nivel' in df_hes.columns:
        fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
        st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
