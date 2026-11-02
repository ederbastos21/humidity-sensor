import csv
import json
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import serial
import serial.tools.list_ports
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = BASE_DIR / "experimentos"
EXPERIMENTS_DIR.mkdir(exist_ok=True)

CSV_COLUMNS = [
    "timestamp",
    "tempo_esp_ms",
    "capacitativo1_raw",
    "capacitativo2_raw",
    "capacitativo3_raw",
    "resistivo1_raw",
    "resistivo2_raw",
    "resistivo3_raw",
]


class SerialCollector:
    """Le a serial em uma thread e grava os dados quando um experimento esta ativo."""

    def __init__(self):
        self.lock = threading.RLock()
        self.serial_conn = None
        self.thread = None
        self.running = False
        self.connected = False
        self.recording = False
        self.paused = False
        self.file_handle = None
        self.csv_writer = None
        self.experiment_dir = None
        self.metadata = None
        self.last_values = None
        self.last_timestamp = None
        self.last_error = None
        self.total_received = 0
        self.total_saved = 0
        self.history = deque(maxlen=600)

    def connect(self, port, baud=115200):
        with self.lock:
            if self.connected:
                return
            self.serial_conn = serial.Serial(port, baudrate=baud, timeout=0.25)
            # Descarta bytes antigos que eventualmente ficaram no buffer.
            self.serial_conn.reset_input_buffer()
            self.connected = True
            self.running = True
            self.last_error = None
            self.thread = threading.Thread(target=self._reader_loop, daemon=True)
            self.thread.start()

    def disconnect(self):
        with self.lock:
            self.running = False
            self.recording = False
            self.paused = False
            self._close_experiment_locked("interrompido")
            conn = self.serial_conn
            self.serial_conn = None
            self.connected = False
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def _reader_loop(self):
        while True:
            with self.lock:
                if not self.running:
                    break
                conn = self.serial_conn

            try:
                raw_line = conn.readline()
                if not raw_line:
                    continue
                line = raw_line.decode("utf-8", errors="ignore").strip()
                parts = line.split(",")
                if len(parts) != 7:
                    continue

                values = [int(part.strip()) for part in parts]
                received_at = datetime.now().astimezone()
                row = {
                    "timestamp": received_at.isoformat(timespec="seconds"),
                    "tempo_esp_ms": values[0],
                    "capacitativo1_raw": values[1],
                    "capacitativo2_raw": values[2],
                    "capacitativo3_raw": values[3],
                    "resistivo1_raw": values[4],
                    "resistivo2_raw": values[5],
                    "resistivo3_raw": values[6],
                }

                with self.lock:
                    self.last_values = row
                    self.last_timestamp = received_at
                    self.total_received += 1
                    self.history.append(row)

                    if self.recording and not self.paused and self.csv_writer:
                        self.csv_writer.writerow(row)
                        self.file_handle.flush()
                        self.total_saved += 1

            except (ValueError, UnicodeDecodeError):
                # Ignora linhas de inicializacao/erro que nao sejam CSV valido.
                continue
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
                    self.running = False
                    self.connected = False
                break

    def start_experiment(self, name, description, interval_seconds):
        with self.lock:
            if not self.connected:
                raise RuntimeError("Conecte o ESP32 antes de iniciar.")
            if self.recording:
                raise RuntimeError("Ja existe um experimento em andamento.")

            now = datetime.now().astimezone()
            safe_name = "".join(
                ch if ch.isalnum() or ch in "-_" else "_"
                for ch in name.strip()
            ).strip("_")
            if not safe_name:
                safe_name = "experimento"

            folder_name = now.strftime("%Y%m%d_%H%M%S") + "_" + safe_name
            folder = EXPERIMENTS_DIR / folder_name
            folder.mkdir(parents=True, exist_ok=False)

            csv_path = folder / "dados.csv"
            self.file_handle = csv_path.open("w", newline="", encoding="utf-8")
            self.csv_writer = csv.DictWriter(self.file_handle, fieldnames=CSV_COLUMNS)
            self.csv_writer.writeheader()
            self.file_handle.flush()

            self.experiment_dir = folder
            self.metadata = {
                "nome": name.strip() or "Experimento",
                "descricao": description.strip(),
                "inicio": now.isoformat(timespec="seconds"),
                "fim": None,
                "status": "em_andamento",
                "porta_serial": self.serial_conn.port,
                "baud_rate": 115200,
                "intervalo_configurado_esp_ms": None,
                "intervalo_exibido_segundos": interval_seconds,
                "sensores": [
                    "capacitativo1", "capacitativo2", "capacitativo3",
                    "resistivo1", "resistivo2", "resistivo3"
                ],
                "observacao": "Valores Raw originais do ADS1115; calibracao ainda nao aplicada."
            }
            self._write_metadata_locked()
            self.recording = True
            self.paused = False
            self.total_saved = 0
            return folder

    def pause(self):
        with self.lock:
            if self.recording:
                self.paused = True
                self.metadata["status"] = "pausado"
                self._write_metadata_locked()

    def resume(self):
        with self.lock:
            if self.recording:
                self.paused = False
                self.metadata["status"] = "em_andamento"
                self._write_metadata_locked()

    def finish(self):
        with self.lock:
            self._close_experiment_locked("finalizado")

    def _write_metadata_locked(self):
        if self.experiment_dir and self.metadata:
            path = self.experiment_dir / "metadados.json"
            temp_path = self.experiment_dir / "metadados.tmp"
            temp_path.write_text(
                json.dumps(self.metadata, ensure_ascii=False, indent=4),
                encoding="utf-8"
            )
            temp_path.replace(path)

    def _close_experiment_locked(self, final_status):
        if self.file_handle:
            try:
                self.file_handle.flush()
                self.file_handle.close()
            except Exception:
                pass
        self.file_handle = None
        self.csv_writer = None

        if self.metadata and self.experiment_dir:
            self.metadata["fim"] = datetime.now().astimezone().isoformat(timespec="seconds")
            self.metadata["status"] = final_status
            self._write_metadata_locked()

        self.recording = False
        self.paused = False

    def snapshot(self):
        with self.lock:
            return {
                "connected": self.connected,
                "recording": self.recording,
                "paused": self.paused,
                "last_values": self.last_values,
                "history": list(self.history),
                "last_error": self.last_error,
                "total_received": self.total_received,
                "total_saved": self.total_saved,
                "experiment_dir": str(self.experiment_dir) if self.experiment_dir else None,
                "metadata": dict(self.metadata) if self.metadata else None,
            }


st.set_page_config(page_title="Monitoramento de umidade", page_icon="🌱", layout="wide")
st.title("Monitoramento de umidade do solo")
st.caption("ESP32 + dois ADS1115 | Aquisição serial, gravação CSV e gráficos em tempo real")

if "collector" not in st.session_state:
    st.session_state.collector = SerialCollector()

collector = st.session_state.collector

with st.sidebar:
    st.header("Conexão")
    ports = list(serial.tools.list_ports.comports())
    port_options = [p.device for p in ports]
    if not port_options:
        port_options = ["/dev/ttyUSB0", "/dev/ttyACM0"]

    selected_port = st.selectbox("Porta serial", port_options)
    st.caption("Feche o Monitor Serial do PlatformIO antes de conectar.")
    if not collector.snapshot()["connected"]:
        if st.button("Conectar ESP32", type="primary", use_container_width=True):
            try:
                collector.connect(selected_port, 115200)
                st.success(f"Conectado em {selected_port}")
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao conectar: {exc}")
    else:
        st.success("ESP32 conectado")
        if st.button("Desconectar", use_container_width=True):
            collector.disconnect()
            st.rerun()

    st.divider()
    st.header("Novo experimento")
    exp_name = st.text_input("Nome", value="Monitoramento do solo")
    exp_description = st.text_area("Descrição", placeholder="Descreva as condições do experimento...")
    interval = st.number_input("Intervalo configurado no ESP32 (segundos)", min_value=1, value=2, step=1)
    if st.button("Iniciar experimento", type="primary", use_container_width=True):
        try:
            folder = collector.start_experiment(exp_name, exp_description, interval)
            st.success(f"Gravando em: {folder}")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    state = collector.snapshot()
    if state["recording"]:
        if state["paused"]:
            if st.button("Retomar gravação", use_container_width=True):
                collector.resume()
                st.rerun()
        else:
            if st.button("Pausar gravação", use_container_width=True):
                collector.pause()
                st.rerun()
        if st.button("Finalizar experimento", type="secondary", use_container_width=True):
            collector.finish()
            st.success("Experimento finalizado e arquivos fechados.")
            st.rerun()

    st.divider()
    st.caption("Os arquivos ficam na pasta `experimentos/` deste projeto.")

@st.fragment(run_every=1)
def live_dashboard():
    data = collector.snapshot()

    if data["last_error"]:
        st.error(f"Erro na comunicação serial: {data['last_error']}")

    if data["connected"]:
        st.success("Comunicação serial ativa")
    else:
        st.warning("ESP32 desconectado")

    if data["recording"] and data["paused"]:
        st.info("Experimento pausado: os dados continuam sendo recebidos, mas não são gravados.")
    elif data["recording"]:
        st.success("Experimento em gravação")
    else:
        st.info("Nenhum experimento está sendo gravado.")

    values = data["last_values"]
    if values:
        st.subheader("Leituras atuais (Raw)")
        sensor_cols = [
            ("Capacitivo 1", "capacitativo1_raw"),
            ("Capacitivo 2", "capacitativo2_raw"),
            ("Capacitivo 3", "capacitativo3_raw"),
            ("Resistivo 1", "resistivo1_raw"),
            ("Resistivo 2", "resistivo2_raw"),
            ("Resistivo 3", "resistivo3_raw"),
        ]
        cols = st.columns(3)
        for idx, (label, key) in enumerate(sensor_cols):
            cols[idx % 3].metric(label, values[key])

        st.caption(f"Última leitura recebida: {values['timestamp']}")
    else:
        st.info("Aguardando a primeira linha válida enviada pelo ESP32...")

    st.subheader("Gráficos em tempo real")
    history = data["history"]
    if history:
        df = pd.DataFrame(history)
        df["amostra"] = range(1, len(df) + 1)
        selected_sensors = st.multiselect(
            "Sensores exibidos",
            options=CSV_COLUMNS[2:],
            default=CSV_COLUMNS[2:],
            format_func=lambda x: x.replace("_raw", "").replace("capacitativo", "Capacitivo ").replace("resistivo", "Resistivo ")
        )
        if selected_sensors:
            long_df = df.melt(
                id_vars=["amostra"],
                value_vars=selected_sensors,
                var_name="sensor",
                value_name="Raw"
            )
            fig = px.line(long_df, x="amostra", y="Raw", color="sensor",
                          title="Evolução das leituras recebidas")
            fig.update_layout(xaxis_title="Número da amostra", yaxis_title="Valor Raw")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Selecione ao menos um sensor para exibir o gráfico.")
    else:
        st.info("O gráfico aparecerá assim que chegarem leituras válidas.")

    st.subheader("Estado da gravação")
    c1, c2 = st.columns(2)
    c1.metric("Linhas recebidas", data["total_received"])
    c2.metric("Linhas gravadas", data["total_saved"])
    if data["experiment_dir"]:
        st.code(data["experiment_dir"], language=None)

live_dashboard()

st.divider()
st.caption("Os valores apresentados são Raw do ADS1115. Faça a calibração individual dos sensores antes de interpretar os valores como porcentagem de umidade.")
