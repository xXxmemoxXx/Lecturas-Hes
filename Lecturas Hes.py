import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
from sqlalchemy import create_engine
import urllib.parse

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(
    page_title="MIAA - Tablero de Consumos", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# 2. ESTILO CSS PARA RÉPLICA VISUAL OSCURA
st.markdown("""
    <style>
    .stApp { background-color: #000b16; color: #ffffff; }
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; font-weight: bold; }
    section[data-testid="stSidebar"] { background-color: #001529; border-right: 1px solid #00d4ff; }
    .stDataFrame { border: 1px solid #00d4ff; border-radius: 5px; }
    h1, h2, h3 { color: #ffffff; border-bottom: 1px solid #00d4ff; padding-bottom: 5px; }
    .stButton>button { background-color: #1a1a1a; color: #00d4ff; border: 1px solid #00d4ff; width: 100%; }
    </style>
    """, unsafe_allow_html=True)

# 3. CONEXIÓN A BASE DE DATOS
@st.cache_resource
def get_engine():
    user = "miaamx_telemetria2"
    password = urllib.parse.quote_plus("bWkrw1Uum1O&")
    host = "miaa.mx"
    db = "miaamx_telemetria2"
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

# 4. CARGA Y LIMPIEZA DE DATOS (PROTECCIÓN CONTRA TYPEERROR)
@st.cache_data(ttl=300)
def load_data():
    engine = get_engine()
    # Tu consulta personalizada optimizada
    query = """
    SELECT 
        t1.Medidor, t1.Fecha, t1.Lectura, t1.Consumo_diario,
        t1.Colonia, t1.Sector, t1.Latitud, t1.Longitud, t1.Giro, t1.Nivel
    FROM HES t1
    ORDER BY t1.Fecha DESC
    LIMIT 3000;
    """
    df = pd.read_sql(query, engine)
    
    # --- LIMPIEZA DE COORDENADAS ---
    df['Latitud'] = pd.to_numeric(df['Latitud'], errors='coerce')
    df['Longitud'] = pd.to_numeric(df['Longitud'], errors='coerce')
    df = df.dropna(subset=['Latitud', 'Longitud'])
    
    # --- ASIGNACIÓN DE COLORES ---
    def set_color(row):
        val = row['Consumo_diario']
        if val <= 0: return [255, 255, 255, 180]    # Blanco (Cero)
        if val < 0.5: return [255, 165, 0, 180]   # Naranja (Bajo)
        if val < 2.0: return [0, 255, 0, 180]     # Normal (Verde)
        return [255, 0, 0, 180]                    # Alto (Rojo)

    df['color'] = df.apply(set_color, axis=1)
    
    # --- CONVERSIÓN DE TIPOS PARA EVITAR ERROR JSON ---
    # Convertimos todo a tipos estándar de Python (float, str)
    df['Medidor'] = df['Medidor'].astype(str)
    df['Consumo_diario'] = df['Consumo_diario'].astype(float)
    df['Lectura'] = df['Lectura'].astype(float)
    df['Colonia'] = df['Colonia'].astype(str)
    df['Giro'] = df['Giro'].astype(str)
    
    return df

# EJECUCIÓN DE CARGA
try:
    data = load_data()
except Exception as e:
    st.error(f"Error de conexión: {e}")
    st.stop()

# 5. BARRA LATERAL (SIDEBAR)
with st.sidebar:
    st.title("⚙️ Filtros")
    f_medidor = st.selectbox("Medidor", ["Todos"] + sorted(list(data['Medidor'].unique())))
    f_colonia = st.selectbox("Colonia", ["Todos"] + sorted(list(data['Colonia'].unique())))
    
    st.markdown("---")
    st.error("⚠️ Informe alarmas")
    st.write("**Ranking Top Consumo**")
    ranking_df = data.nlargest(10, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.table(ranking_df)

# APLICAR FILTROS SI ES NECESARIO
df_final = data.copy()
if f_medidor != "Todos":
    df_final = df_final[df_final['Medidor'] == f_medidor]
if f_colonia != "Todos":
    df_final = df_final[df_final['Colonia'] == f_colonia]

# 6. LAYOUT PRINCIPAL (INDICADORES)
st.title("📊 Medidores Inteligentes - MIAA")

m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{df_final['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{df_final['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{df_final['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_final):,}")

# 7. MAPA Y TABLA LATERAL
col_izq, col_der = st.columns([3, 1])

with col_izq:
    # DATA ESPECÍFICA PARA EL MAPA (Limpia de objetos MySQL)
    map_payload = df_final[['Latitud', 'Longitud', 'color', 'Medidor', 'Consumo_diario', 'Colonia']].to_dict(orient="records")

    view_state = pdk.ViewState(
        latitude=df_final['Latitud'].median(), 
        longitude=df_final['Longitud'].median(), 
        zoom=11, 
        pitch=45
    )
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        map_payload, # Pasamos dicts planos para evitar error de serialización
        get_position='[Longitud, Latitud]',
        get_color='color',
        get_radius=110,
        pickable=True
    )
    
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style='mapbox://styles/mapbox/dark-v10',
        tooltip={"text": "Medidor: {Medidor}\nConsumo: {Consumo_diario} m3\nColonia: {Colonia}"}
    ))
    
    st.markdown("<p style='text-align: center;'>⚪ CERO | 🟠 BAJO | 🟢 NORMAL | 🔴 ALTO</p>", unsafe_allow_html=True)

with col_der:
    st.write("**Lecturas recientes**")
    st.dataframe(df_final[['Fecha', 'Lectura', 'Consumo_diario']].head(15), hide_index=True)
    
    # Gráfica de Dona
    st.write("**Distribución por Giro**")
    fig = px.pie(df_final, names='Giro', hole=0.7)
    fig.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

# 8. BOTONES INFERIORES
c1, c2, c3 = st.columns([2, 1, 1])
with c2: st.button("Informe Ranking")
with c3: st.button("Reset")
