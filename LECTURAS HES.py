import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import tkinter as tk
from tkinter import scrolledtext, ttk
import threading
import datetime
import time
import sys
import os
import mysql.connector
# IMPORTANTE: Importación explícita para que PyInstaller la detecte
try:
    import psycopg2
except ImportError:
    pass

# =================================================================
# CONFIGURACIÓN PARA EXECUTABLE (LOG DE ERRORES)
# =================================================================
if getattr(sys, 'frozen', False):
    # Si el programa falla, creará un archivo .txt con el error exacto
    ruta_log = os.path.join(os.path.dirname(sys.executable), "error_log.txt")
    sys.stderr = open(ruta_log, "w")
    sys.stdout = open(os.path.join(os.path.dirname(sys.executable), "out_log.txt"), "w")
# =================================================================
# 1. CONFIGURACIÓN DE CONEXIONES
# =================================================================
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

MAPEO_SCADA = {
"P-002": {
"GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS",
"PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
"VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2",
"VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3",
"VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3",
"AMP_L1":"PZ_002_TRC_CORR_L1",
"AMP_L2":"PZ_002_TRC_CORR_L2",
"AMP_L3":"PZ_002_TRC_CORR_L3",
"LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM",
"SUMERGENCIA":"PZ_002_TRC_SUMERG",
"NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
},

"P-003": {
"GASTO_(l.p.s.)":"PZ_003_CAU_INS",
"PRESION_(kg/cm2)":"PZ_003_PRES_INS",
"VOLTAJE_L1":"PZ_003_VOL_L1_L2",
"VOLTAJE_L2":"PZ_003_VOL_L2_L3",
"VOLTAJE_L3":"PZ_003_VOL_L1_L3",
"AMP_L1":"PZ_003_CORR_L1",
"AMP_L2":"PZ_003_CORR_L2",
"AMP_L3":"PZ_003_CORR_L3",
"LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM",
"SUMERGENCIA":"PZ_003_SUMERG",
"NIVEL_DINAMICO":"PZ_003_NIV_EST",
},

}

# --- MAPEO DE COLUMNAS A POSTGRES ---
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)':                  '_Caudal',
    'PRESION_(kg/cm2)':                '_Presion',
    'LONGITUD_DE_COLUMNA':             '_Long_colum',
    'COLUMNA_DIAMETRO_1':              '_Diam_colum',
    'TIPO_COLUMNA':                    '_Tipo_colum',
    'SECTOR_HIDRAULICO':               '_Sector',
    'NIVEL_DINAMICO_(mts)':            '_Nivel_Din',
    'NIVEL_ESTATICO_(mts)':            '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)':         '_Vm_estr',
    'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1':                      '_Distrito',
    'ESTATUS':                         '_Estatus',
    'TELEMETRIA':                      '_Telemetria',
    'FECHA_ACTUALIZACION':             '_Ultima_actualizacion',
}


# =================================================================
# 2. FUNCIONES DE LÓGICA
# =================================================================

def limpiar_dato_para_postgres(valor):
    """Quita comas para evitar errores de tipo double precision en Postgres."""
    if pd.isna(valor) or valor == "" or str(valor).lower() == "nan": return None
    if isinstance(valor, str):
        v = valor.replace(',', '').strip()
        try: return float(v)
        except: return valor
    return valor

def obtener_gateids(tags):
    """Busca los IDs de los tags usando los nombres correctos de argumentos."""
    try:
        conn = mysql.connector.connect(**DB_SCADA, use_pure=True, autocommit=True)
        cursor = conn.cursor()
        format_strings = ','.join(['%s'] * len(tags))
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({format_strings})", list(tags))
        dic = {name: gid for name, gid in cursor.fetchall()}
        cursor.close(); conn.close()
        return dic
    except Exception as e: 
        print(f"⚠️ Error GATEIDs: {e}")
        return {}

def obtener_valores_scada(gateids):
    """Obtiene valores de los últimos 15 min por ID."""
    if not gateids: return {}
    try:
        conn = mysql.connector.connect(**DB_SCADA, use_pure=True, autocommit=True)
        cursor = conn.cursor()
        ids = list(gateids.values())
        format_strings = ','.join(['%s'] * len(ids))
        query = f"""SELECT GATEID, VALUE FROM vfitagnumhistory 
                    WHERE GATEID IN ({format_strings}) 
                    AND FECHA >= NOW() - INTERVAL 15 MINUTE 
                    ORDER BY FECHA DESC"""
        cursor.execute(query, ids)
        data = {}
        for gid, val in cursor.fetchall():
            if gid not in data: data[gid] = val
        cursor.close(); conn.close()
        return data
    except Exception as e: 
        print(f"⚠️ Error SCADA: {e}")
        return {}

# =================================================================
# 3. PROCESO DE ACTUALIZACIÓN
# =================================================================

def ejecutar_actualizacion(app_ref):
    def ui_upd(val):
        app_ref.root.after(0, lambda: app_ref.progress.config(value=val))
        app_ref.root.after(0, lambda: app_ref.lbl_porcentaje.config(text=f"{val}%"))

    try:
        ui_upd(10)
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        print(f"✅ Google Sheets: {len(df)} registros leídos.")

        # --- FASE SCADA ---
        ui_upd(30)
        tags_req = set(tag for p in MAPEO_SCADA.values() for tag in p.values() if tag)
        id_map = obtener_gateids(tags_req)
        val_map = obtener_valores_scada(id_map)
        
        for p_id, config in MAPEO_SCADA.items():
            for col, tag in config.items():
                val = val_map.get(id_map.get(tag))
                if val is not None:
                    try:
                        f_val = float(str(val).replace(',', ''))
                        if f_val != 0: df.loc[df['POZOS'] == p_id, col] = round(f_val, 2)
                    except: pass
        print("📡 SCADA: Valores inyectados en DataFrame.")

        # --- FASE INFORME (MYSQL miaamx_telemetria2) ---
        ui_upd(60)
        print("💾 Actualizando tabla INFORME en MySQL...")
        # Nota: SQLAlchemy usa 'password', no 'pass'
        engine_inf = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{urllib.parse.quote_plus(DB_INFORME['password'])}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        
        with engine_inf.begin() as conn_inf:
            conn_inf.execute(text("TRUNCATE TABLE INFORME"))
            # Obtenemos columnas de la tabla real para no intentar insertar columnas del CSV que no existan en DB
            res = conn_inf.execute(text("SHOW COLUMNS FROM INFORME"))
            db_cols = [r[0] for r in res]
            df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
            df_to_save.to_sql('INFORME', con=conn_inf, if_exists='append', index=False)
        print("✅ MySQL: Tabla INFORME actualizada.")

        # --- FASE POSTGRES ---
        ui_upd(85)
        print("🐘 Sincronizando PostgreSQL (QGIS)...")
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        url_pg = f"postgresql://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}"
        engine_pg = create_engine(url_pg)
        
        with engine_pg.begin() as conn_pg:
            for _, row in df.iterrows():
                id_m = str(row['ID']).strip()
                if not id_m or id_m == "nan": continue
                set_c = []; params = {"id": id_m}
                for c_csv, c_pg in MAPEO_POSTGRES.items():
                    if c_csv in df.columns:
                        params[c_pg] = limpiar_dato_para_postgres(row[c_csv])
                        set_c.append(f'"{c_pg}" = :{c_pg}')
                if set_c:
                    conn_pg.execute(text(f'UPDATE public."Pozos" SET {", ".join(set_c)} WHERE "ID" = :id'), params)
        
        ui_upd(100)
        print(f"🚀 TODO OK: {datetime.datetime.now().strftime('%H:%M:%S')}\n")

    except Exception as e:
        print(f"❌ Error crítico: {e}")
    finally:
        time.sleep(2); ui_upd(0)

# =================================================================
# 4. INTERFAZ GRÁFICA (Igual a la tuya)
# =================================================================
class AppSincronizador:
    def __init__(self, root):
        self.root = root; self.root.title("MIAA Data Center"); self.root.geometry("1100x750"); self.activo = False
        
        header = tk.Frame(root, bg="#2c3e50", height=60); header.pack(fill="x")
        tk.Label(header, text="MONITOR DE MYSQL - POSTGRES", bg="#2c3e50", fg="white", font=("Arial", 12, "bold")).pack(pady=15)
        
        frame = tk.LabelFrame(root, text=" Configuración de Tiempo ", padx=15, pady=15); frame.pack(fill="x", padx=15)
        
        # Combo Modo
        self.combo_modo = ttk.Combobox(frame, values=["Diario", "Periódico"], width=12, state="readonly")
        self.combo_modo.current(0); self.combo_modo.grid(row=0, column=0, padx=5)
        self.combo_modo.bind("<<ComboboxSelected>>", self.toggle_inputs)

        # Horas
        tk.Label(frame, text="Hora:").grid(row=0, column=1)
        self.spin_h = tk.Spinbox(frame, from_=0, to=23, width=4, format="%02.0f")
        self.spin_h.grid(row=0, column=2, padx=5)
        
        # Minutos / Intervalo
        tk.Label(frame, text="Min/Int:").grid(row=0, column=3)
        self.spin_m = ttk.Combobox(frame, values=["01", "05", "10", "15", "30", "58"], width=4)
        self.spin_m.set("10"); self.spin_m.grid(row=0, column=4, padx=5)
        
        self.btn_go = tk.Button(frame, text="INICIAR", command=self.start, bg="#27ae60", fg="white", width=12)
        self.btn_go.grid(row=0, column=5, padx=10)
        self.btn_stop = tk.Button(frame, text="PARAR", command=self.stop, state="disabled", bg="#c0392b", fg="white", width=10)
        self.btn_stop.grid(row=0, column=6)
        
        self.progress = ttk.Progressbar(frame, orient="horizontal", length=150, mode="determinate"); self.progress.grid(row=0, column=7, padx=10)
        self.lbl_porcentaje = tk.Label(frame, text="0%", font=("Arial", 10, "bold")); self.lbl_porcentaje.grid(row=0, column=8)
        
        self.timer = tk.Label(root, text="ESPERANDO...", fg="#2980b9", font=("Consolas", 12, "bold")); self.timer.pack(pady=10)
        self.txt = scrolledtext.ScrolledText(root, state='disabled', height=35, bg="black", fg="#00FF00", font=("Consolas", 10))
        self.txt.pack(fill="both", expand=True, padx=15, pady=10); sys.stdout = self

    def toggle_inputs(self, event=None):
        """Desactiva la casilla de horas si el modo es Periódico."""
        if self.combo_modo.get() == "Periódico":
            self.spin_h.delete(0, "end")
            self.spin_h.insert(0, "00")
            self.spin_h.config(state="disabled")
        else:
            self.spin_h.config(state="normal")

    def write(self, t): self.txt.config(state='normal'); self.txt.insert(tk.END, t); self.txt.see(tk.END); self.txt.config(state='disabled')
    def flush(self): pass

    def get_next(self):
        ahora = datetime.datetime.now()
        h = int(self.spin_h.get()) if self.combo_modo.get() == "Diario" else 0
        m = int(self.spin_m.get())
        if self.combo_modo.get() == "Diario":
            obj = ahora.replace(hour=h, minute=m, second=0, microsecond=0)
            if obj <= ahora: obj += datetime.timedelta(days=1)
        else:
            prox_m = ((ahora.minute // m) + 1) * m
            obj = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m)
        return obj

    def update_clock(self):
        if self.activo:
            diff = self.get_next() - datetime.datetime.now()
            s = max(0, int(diff.total_seconds()))
            self.timer.config(text=f"PRÓXIMA CARGA EN: {s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}")
            self.root.after(1000, self.update_clock)

    def start(self): 
        self.activo = True; self.btn_go.config(state="disabled"); self.btn_stop.config(state="normal")
        self.update_clock(); threading.Thread(target=self.loop, daemon=True).start()

    def stop(self): 
        self.activo = False; self.btn_go.config(state="normal"); self.btn_stop.config(state="disabled")
        self.timer.config(text="DETENIDO")

    def loop(self):
        last_exec = None
        while self.activo:
            now = datetime.datetime.now()
            if self.combo_modo.get() == "Diario":
                target = f"{self.spin_h.get().zfill(2)}:{self.spin_m.get().zfill(2)}"
                if now.strftime("%H:%M") == target and last_exec != now.date():
                    threading.Thread(target=ejecutar_actualizacion, args=(self,), daemon=True).start()
                    last_exec = now.date()
            else:
                m_int = int(self.spin_m.get())
                if now.minute % m_int == 0 and last_exec != now.strftime("%H:%M"):
                    threading.Thread(target=ejecutar_actualizacion, args=(self,), daemon=True).start()
                    last_exec = now.strftime("%H:%M")
            time.sleep(1)

if __name__ == "__main__":
    root = tk.Tk(); app = AppSincronizador(root); root.mainloop()