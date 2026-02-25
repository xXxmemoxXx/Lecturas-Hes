import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px

# 1. CONFIGURACIÓN E INTERFAZ (ESTILO EXACTO A LA IMAGEN)

    
    folium_static(m, width=900, height=520)
    st.markdown("<p style='text-align: center;'>🟢 REGULAR | 🟢 NORMAL | 🟠 BAJO | ⚪ CERO | 🔴 MUY ALTO | 🔴 ALTO | 🔵 null</p>", unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(20), hide_index=True, height=400)
    
    # Gráfico de dona (Tarifas)
    fig = px.pie(df_hes, names='Nivel', hole=0.7, color_discrete_sequence=px.colors.qualitative.Safe)
    fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), paper_bgcolor='rgba(0,0,0,0)', height=250)
    st.plotly_chart(fig, use_container_width=True)

st.button("Reset")



