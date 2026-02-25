import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN VISUAL
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")
st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 26px; color: #ffffff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; }
    /* Estilo para los selectores del sidebar */
    div[data-baseweb="select"] > div { background-color: #000000 !important; color: white !important; border: 1px solid #444 !important; }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIONES
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(user='map_tecnica', password='M144.Tec', host='ti.miaa.mx', database='qgis', port='5432')

# 3. LÓGICA DE DATOS Y FILTROS (SIDEBAR)
mysql_engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150)
    st.write("---")
    
    # Rango de fechas
    fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # FILTROS DINÁMICOS (Funciones del menú izquierdo)
        st.write("### Filtros")
        
        # Función para crear selectores que filtran el dataframe
        def crear_filtro(label, columna):
            if columna in df_hes.columns:
                opciones = ["Todos"] + sorted(df_hes[columna].dropna().unique().tolist())
                return st.selectbox(label, opciones)
            return "Todos"

        f_cliente = crear_filtro("ClientID_API", "ClientID_API")
        f_metodo  = crear_filtro("Metodoid_API", "Metodoid_API")
        f_medidor = crear_filtro("Medidor", "Medidor")
        f_predio  = crear_filtro("Predio", "Predio")
        f_colonia = crear_filtro("Colonia", "Colonia")
        f_giro    = crear_filtro("Giro", "Giro")
        f_sector  = crear_filtro("Sector", "Sector")

        # Aplicar filtros al dataframe
        df_filtrado = df_hes.copy()
        filtros = {
            "ClientID_API": f_cliente, "Metodoid_API": f_metodo, "Medidor": f_medidor,
            "Predio": f_predio, "Colonia": f_colonia, "Giro": f_giro, "Sector": f_sector
        }
        for col, val in filtros.items():
            if val != "Todos":
                df_filtrado = df_filtrado[df_filtrado[col] == val]

        # SECCIÓN DE ALARMAS
        st.error("⚠️ Informe alarmas")
        
        # RANKING TOP CONSUMO (Parte inferior del menú izquierdo)
        st.write("**Ranking Top Consumo**")
        if 'Consumo_diario' in df_filtrado.columns:
            ranking = df_filtrado.groupby('Medidor')['Consumo_diario'].sum().nlargest(10).reset_index()
            ranking.columns = ['Medidor', 'm3 Acumulado']
            st.dataframe(ranking, hide_index=True)
            
        # Botón de descarga/informe
        st.button("Generar Informe Detallado", use_container_width=True)

    else:
        st.warning("Seleccione un rango de fechas.")
        st.stop()

# 4. PROCESAMIENTO PARA MAPA (Basado en df_filtrado)
agg_dict = {col: 'first' for col in df_filtrado.columns if col not in ['Medidor', 'Consumo_diario', 'Lectura', 'Fecha']}
agg_dict.update({'Consumo_diario': 'sum', 'Lectura': 'last', 'Fecha': 'last'})
df_mapa = df_filtrado.groupby('Medidor').agg(agg_dict).reset_index()

# 5. DASHBOARD PRINCIPAL (MAPA + DERECHA)
st.title("Medidores inteligentes - Tablero de consumos")

# Métricas superiores
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_mapa['Medidor'].nunique()}")
m2.metric("Consumo acumulado m3", f"{df_filtrado['Consumo_diario'].sum():,.1f}")
m3.metric("Promedio diario m3", f"{df_filtrado['Consumo_diario'].mean():.2f}")
m4.metric("Total Lecturas", f"{len(df_filtrado):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    # Carga de Sectores (Postgres)
    try:
        pg_conn = get_postgres_conn()
        df_sec = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', pg_conn)
        pg_conn.close()
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)
    except: pass

    # Puntos con el Popup detallado que ya teníamos
    for _, r in df_mapa.iterrows():
        # (Aquí va tu función get_color_logic y la construcción del pop_html idéntica a la anterior)
        # Por brevedad uso un color fijo, pero mantén tu lógica de colores ya probada.
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']], 
            radius=4, color="#00FF00", fill=True, popup="Ficha Técnica"
        ).add_to(m)
    
    folium_static(m, width=900, height=550)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO</p>", unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_filtrado[['Fecha', 'Lectura', 'Consumo_diario']].tail(20), hide_index=True)
    
    fig = px.pie(df_filtrado, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")
