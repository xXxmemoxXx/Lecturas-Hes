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

# 2. ESTILO CSS PARA RÉPLICA VISUAL (Look Cyberpunk/Dark)
st.markdown("""
    <style>
    /* Fondo principal y textos */
    .stApp { background-color: #000b16; color: #ffffff; }
    
    /* Contenedores de métricas superiores */
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; font-weight: bold; }
    [data-testid="stMetricLabel"] { color: #ffffff; }
    
    /* Personalización de Sidebar */
    section[data-testid="stSidebar"] { background-color: #001529; border-right: 1px solid #00d4ff; }
    
    /* Estilo para tablas y DataFrames */
    .stDataFrame { border: 1px solid #00d4ff; border-radius: 5px; }
    
    /* Títulos de sección */
    h1, h2, h3 { color: #ffffff; border-bottom: 1px solid #00d4ff; padding-bottom: 5px; }
    
    /* Botones estilo tablero */
    .stButton>button {
        background-color: #1a1a1a;
        color: #00d4ff;
        border: 1px solid #00d4ff;
        width: 100%;
    }
    </style>
    """, unsafe_allow_html=True)

# 3. CONEXIÓN A BASE DE DATOS
@st.cache_resource
def get_engine():
    user = "miaamx_telemetria2"
    # Escapar caracteres especiales en la contraseña
    password = urllib.parse.quote_plus("bWkrw1Uum1O&")
    host = "miaa.mx"
    db = "miaamx_telemetria2"
    # Usar mysql-connector como driver
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

# 4. CARGA Y LIMPIEZA DE DATOS (Solución al TypeError)
@st.cache_data(ttl=300)
def load_data():
    engine = get_engine()
    # Tu consulta personalizada para traer los datos y acumulados mensuales
    query = """
    SELECT 
        t1.*, 
        agg.Volumen_Mensual, 
        agg.Ultima_Fecha, 
        agg.Ultima_Lectura
    FROM HES t1
    LEFT JOIN (
        SELECT 
            Medidor, YEAR(Fecha) as Anio, MONTH(Fecha) as Mes,
            SUM(Consumo_diario) as Volumen_Mensual,
            MAX(Fecha) as Ultima_Fecha,
            MAX(Lectura) as Ultima_Lectura
        FROM HES
        GROUP BY Medidor, YEAR(Fecha), MONTH(Fecha)
    ) agg ON t1.Medidor = agg.Medidor 
      AND YEAR(t1.Fecha) = agg.Anio 
      AND MONTH(t1.Fecha) = agg.Mes
    ORDER BY t1.Fecha DESC
    LIMIT 2000;
    """
    df = pd.read_sql(query, engine)
    
    # Limpieza estricta para Pydeck
    df['Latitud'] = pd.to_numeric(df['Latitud'], errors='coerce')
    df['Longitud'] = pd.to_numeric(df['Longitud'], errors='coerce')
    df = df.dropna(subset=['Latitud', 'Longitud'])
    
    # Lógica de colores basada en la leyenda de la imagen
    def set_color(row):
        val = row['Consumo_diario']
        if val <= 0: return [255, 255, 255, 200]    # Cero (Blanco)
        if val < 0.5: return [255, 165, 0, 200]   # Bajo (Naranja)
        if val < 2.0: return [0, 255, 0, 200]     # Normal (Verde)
        return [255, 0, 0, 200]                    # Alto (Rojo)

    df['color'] = df.apply(set_color, axis=1)
    return df

# --- INICIO DE LA APLICACIÓN ---
try:
    data = load_data()
except Exception as e:
    st.error(f"Error de conexión: {e}")
    st.stop()

# 5. BARRA LATERAL (Filtros y Alarmas)
with st.sidebar:
    st.title("⚙️ Filtros")
    f_fecha = st.date_input("Rango de consulta")
    f_medidor = st.selectbox("Medidor", ["Todos"] + list(data['Medidor'].unique()))
    f_colonia = st.selectbox("Colonia", ["Todos"] + list(data['Colonia'].unique()))
    f_sector = st.selectbox("Sector", ["Todos"] + list(data['Sector'].unique()))
    
    st.markdown("---")
    st.error("⚠️ Informe alarmas")
    st.write("**Ranking Top Consumo**")
    # Ranking visual con barra
    ranking_df = data.nlargest(8, 'Consumo_diario')[['Medidor', 'Consumo_diario']]
    st.dataframe(ranking_df, hide_index=True)

# 6. CUERPO PRINCIPAL (Layout de la imagen)
st.subheader("Medidores inteligentes - Tablero de consumos")

# Fila de métricas superiores
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{data['Medidor'].nunique():,}")
m2.metric("Consumo acumulado m3", f"{data['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. Consumo diario m3", f"{data['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(data):,}")

# Distribución: Mapa (Izquierda) y Datos/Gráfica (Derecha)
col_izq, col_der = st.columns([3, 1])

with col_izq:
    # Configuración del mapa Pydeck
    view_state = pdk.ViewState(
        latitude=data['Latitud'].median(), 
        longitude=data['Longitud'].median(), 
        zoom=12, pitch=40
    )
    
    layer = pdk.Layer(
        "ScatterplotLayer",
        data,
        get_position='[Longitud, Latitud]',
        get_color='color',
        get_radius=100,
        pickable=True
    )
    
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style='mapbox://styles/mapbox/dark-v10',
        tooltip={"text": "Medidor: {Medidor}\nConsumo: {Consumo_diario} m3\nColonia: {Colonia}"}
    ))
    
    st.markdown("""
    <div style='display: flex; justify-content: space-around; font-size: 12px;'>
        <span>⚪ CERO</span><span>🟠 BAJO</span><span>🟢 NORMAL</span><span>🔴 ALTO</span>
    </div>
    """, unsafe_allow_html=True)

with col_der:
    st.write("**Consumo real**")
    st.dataframe(
        data[['Fecha', 'Lectura', 'Consumo_diario']].head(12), 
        hide_index=True,
        height=350
    )
    
    # Gráfica de Dona (Pie chart)
    st.write("**Distribución por Nivel**")
    fig = px.pie(data, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Set3)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)

# Botones inferiores
b1, b2, b3 = st.columns([2, 1, 1])
with b2: st.button("Informe Ranking")
with b3: st.button("Reset")
