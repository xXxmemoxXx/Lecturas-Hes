# --- RANKING TOP CONSUMO ---
        st.write("**Ranking Top ... Consumo...**")
        if not df_hes.empty:
            # Agrupamos por medidor para el ranking lateral
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            
            for _, row in ranking_data.iterrows():
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"<span style='color: #81D4FA; font-size: 13px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                
                # Barra de progreso roja y valor
                pct = (row['Consumo_diario'] / max_c) * 100
                c2.markdown(f"""
                    <div style="display: flex; align-items: center; justify-content: flex-end;">
                        <span style="font-size: 12px; margin-right: 5px;">{row['Consumo_diario']:,.0f}</span>
                        <div style="width: 40px; background-color: #333; height: 8px; border-radius: 2px;">
                            <div style="width: {pct}%; background-color: #FF0000; height: 8px; border-radius: 2px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
