import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
from sqlalchemy import create_engine
import urllib.parse

# Configuración de página estilo "Wide" y título
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide", initial_sidebar_state="expanded")

# --- ESTILO CSS PERSONALIZADO (Para lograr el look de la imagen) ---
st.markdown("""
    <style>
    .main { background-color: #000000; color: #ffffff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #00d4ff; }
    .stDataFrame { border: 1px solid #00d4ff; }
    /* Estilo para los contenedores de métricas superiores */
    .metric-container {
        background-color: rgba(0, 212, 255, 0.1);
        border: 1px solid #00d4ff;
        padding: 10px;
        border-radius: 5px;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)

# --- CONEXIÓN A BASE DE DATOS ---
@st.cache_resource
def get_engine():
    user = "miaamx_telemetria2"
    password = urllib.parse.quote_plus("bWkrw1Uum1O&")
    host = "miaa.mx"
    db = "miaamx_telemetria2"
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

# --- LÓGICA DE DATOS ---
engine = get_engine()

@st.cache_data(ttl=600)
def load_data():
    query = "SELECT * FROM HES ORDER BY Fecha DESC LIMIT 5000"
    df = pd.read_sql(query, engine)
    # Lógica de colores basada en consumo (Simulando la leyenda de la imagen)
    def color_picker(row):
        if row['Consumo_diario'] <= 0: return [255, 255, 255, 150]    # Cero (Blanco)
        if row['Consumo_diario'] < 0.5: return [255, 165, 0, 150]    # Bajo (Naranja)
        if row['Consumo_diario'] < 2.0: return [0, 255, 0, 150]      # Normal (Verde)
        return [255, 0, 0, 150]                                      # Muy Alto (Rojo)
    
    df['fill_color'] = df.apply(color_picker, axis=1)
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Error al conectar: {e}")
    st.stop()

# --- ESTRUCTURA DEL TABLERO ---

# 1. BARRA LATERAL (FILTROS)
with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=150) # Ajustar URL si es necesario
    st.date_input("Rango de Fechas")
    st.selectbox("ClientID_API", ["Todos"] + list(df['ClienteID_API'].unique()))
    st.selectbox("Medidor", ["Todos"] + list(df['Medidor'].unique()))
    st.selectbox("Colonia", ["Todos"] + list(df['Colonia'].unique()))
    st.selectbox("Sector", ["Todos"] + list(df['Sector'].unique()))
    
    st.warning("⚠️ Informe alarmas")
    st.write("Ranking Top Consumo")
    top_ranking = df.nlargest(5, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.table(top_ranking)

# 2. CUERPO PRINCIPAL
# Indicadores Superiores
m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("N° de medidores", f"{df['Medidor'].nunique():,}")
with m2: st.metric("Consumo acumulado m3", f"{df['Consumo_diario'].sum():,.1f}")
with m3: st.metric("Prom. Consumo diario m3", f"{df['Consumo_diario'].mean():.2f}")
with m4: st.metric("Lecturas", f"{len(df):,}")

# Fila Central: Mapa y Tabla Derecha
col_mapa, col_derecha = st.columns([3, 1])

with col_mapa:
    # Configuración del Mapa (Pydeck)
    view_state = pdk.ViewState(latitude=21.8853, longitude=-102.2916, zoom=12, pitch=45)
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        get_position='[Longitud, Latitud]',
        get_color='fill_color',
        get_radius=80,
        pickable=True,
    )
    
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style='mapbox://styles/mapbox/dark-v10',
        tooltip={"text": "Medidor: {Medidor}\nConsumo: {Consumo_diario} m3"}
    ))
    
    st.caption("🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO")

with col_derecha:
    st.subheader("Lecturas Reales")
    st.dataframe(df[['Fecha', 'Lectura', 'Consumo_diario']].head(15), height=400)
    
    # Gráfica de Dona (Distribución por Giro/Sector)
    st.subheader("Distribución")
    fig = px.pie(df, names='Giro', hole=0.6, 
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

# Botones de acción inferiores
c1, c2, c3 = st.columns([2, 1, 1])
with c2: st.button("Informe Ranking", use_container_width=True)
with c3: st.button("Reset", use_container_width=True)
