import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN E INTERFAZ
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #000000 !important; color: white; }
    section[data-testid="stSidebar"] { background-color: #000b16 !important; border-right: 1px solid #00d4ff; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #ffffff; font-weight: bold; }
    .stDataFrame { border: 1px solid #00d4ff; background-color: #000000; }
    /* Ajuste para calendario en español y selects */
    div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {
        background-color: #1a1a1a !important; color: white !important; border: 1px solid #444 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# 2. CONEXIÓN
@st.cache_resource
def get_mysql_engine():
    pwd = urllib.parse.quote_plus("bWkrw1Uum1O&")
    return create_engine(f"mysql+mysqlconnector://miaamx_telemetria2:{pwd}@miaa.mx/miaamx_telemetria2")

# 3. LÓGICA DE COLOR POR CONSUMO ACUMULADO DEL MES
def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {
        "REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", 
        "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"
    }
    config = {
        'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30],
        'DOMESTICO C': [8, 19, 37, 50], 'COMERCIAL': [5, 10, 40, 60],
        'ESTATAL PUBLICO': [17, 56, 143, 200], 'FEDERAL PUBLICO': [16, 68, 183, 200],
        'MUNICIPAL PUBLICO': [28, 72, 157, 200]
    }
    if v <= 0: return colors["CERO"]
    lim = config.get(nivel, config['DOMESTICO A'])
    if v <= lim[0]: return colors["BAJO"]
    if v <= lim[1]: return colors["REGULAR"]
    if v <= lim[2]: return colors["NORMAL"]
    if v <= lim[3]: return colors["ALTO"]
    return colors["MUY ALTO"]

# 4. CARGA DE DATOS
engine = get_mysql_engine()

with st.sidebar:
    st.image("https://miaa.mx/assets/img/logo_miaa.png", width=120)
    
    # Calendario Manual en Español
    st.write("### Periodo de consulta")
    f_inicio = st.date_input("Fecha de inicio", value=pd.Timestamp(2026, 2, 1))
    f_fin = st.date_input("Fecha de finalización", value=pd.Timestamp(2026, 2, 28))
    
    query = f"SELECT * FROM HES WHERE Fecha BETWEEN '{f_inicio}' AND '{f_fin}'"
    df_full = pd.read_sql(query, engine)

    # Filtros laterales
    for col in ["ClientID_API", "MetodoID_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]:
        st.selectbox(col, ["Todos"] + sorted(df_full[col].unique().tolist()) if col in df_full.columns else ["Todos"])

    st.error("⚠️ Informe alarmas")
    st.write("**Ranking Top Consumo**")
    top_10 = df_full.groupby('Medidor')['Consumo_diario'].sum().nlargest(10).reset_index()
    st.dataframe(top_10, hide_index=True)

# 5. CÁLCULO PARA EL MAPA (POR MEDIDOR)
# Agrupamos para obtener el consumo acumulado del mes por medidor
df_mapa = df_full.groupby('Medidor').agg({
    'Consumo_diario': 'sum', # Consumo Acumulado del Mes para el Color
    'Lectura': 'last',       # Última lectura conocida
    'Fecha': 'last',
    'Latitud': 'first',
    'Longitud': 'first',
    'Nivel': 'first',
    'ClientID_API': 'first',
    'Nombre': 'first',
    'Predio': 'first',
    'Domicilio': 'first',
    'Colonia': 'first',
    'Giro': 'first',
    'Primer_instalacion': 'first'
}).reset_index()

# 6. LAYOUT PRINCIPAL
st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", "4.664")
m2.metric("Consumo acumulado m3", f"{df_full['Consumo_diario'].sum():,.1f}")
m3.metric("Prom. de Consumo diario m3", f"{df_full['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_full):,}")

col_map, col_real = st.columns([3, 1.2])

with col_map:
    # Mapa Dark
    mapa = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    
    for _, r in df_mapa.iterrows():
        # El color ahora se define por el consumo ACUMULADO del mes
        color_p = get_color_logic(r['Nivel'], r['Consumo_diario'])
        
        # POPUP DETALLADO (Igual a image_9a0619.png / image_a57246.png)
        html_popup = f"""<div style="font-family: Arial; font-size: 11px; width: 280px; color: #333;">
            <b>Cliente:</b> {r['ClientID_API']} - <b>Serie:</b> {r['Medidor']}<br>
            <b>Fecha instalacion:</b> {r['Primer_instalacion']}<br>
            <b>Predio:</b> {r['Predio']}<br>
            <b>Nombre:</b> {r['Nombre']}<br>
            <b>Tarifa:</b> {r['Nivel']}<br>
            <b>Giro:</b> {r['Giro']}<br>
            <b>Dirección:</b> {r['Domicilio']} - <b>Colonia:</b> {r['Colonia']}<br>
            <b>Lectura:</b> {r['Lectura']} (m3) - <b>Última:</b> {r['Fecha']}<br>
            <b>Consumo mes:</b> {r['Consumo_diario']:.2f} (m3)<br>
            <b>Comunicación:</b> Lorawan
        </div>"""
        
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']],
            radius=4, color=color_p, fill=True, fill_opacity=0.9,
            popup=folium.Popup(html_popup, max_width=300)
        ).add_to(mapa)
    
    folium_static(mapa, width=900, height=550)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)
    st.button("Informe Ranking", use_container_width=True)

with col_real:
    st.write("🟢 **Consumo real**")
    # Tabla lateral con íconos de antena (Lorawan)
    df_resumen = df_full[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).copy()
    df_resumen.columns = ['Fecha', 'Lectura', 'm3']
    st.dataframe(df_resumen, hide_index=True, height=450)
    
    # Gráfico de dona por Nivel
    fig = px.pie(df_full, names='Nivel', hole=0.7)
    fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("🔵 COMERCIAL | 🟣 DOMESTICO A | 🔴 DOMESTICO B | 🟢 DOMESTICO C")

st.button("Reset")
