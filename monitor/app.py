import atexit
import csv
import json
import shutil
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import serial
import serial.tools.list_ports
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = BASE_DIR / "experimentos"
EXPERIMENTS_DIR.mkdir(exist_ok=True)

BASE_COLUMNS = [
    "timestamp",
    "tempo_esp_ms",
]

SENSOR_COLUMNS = [
    "capacitativo1_raw",
    "capacitativo2_raw",
    "capacitativo3_raw",
    "resistivo1_raw",
    "resistivo2_raw",
    "resistivo3_raw",
]

SENSOR_LABELS = {
    "capacitativo1_raw": "Capacitivo 1",
    "capacitativo2_raw": "Capacitativo 2",
    "capacitativo3_raw": "Capacitativo 3",
    "resistivo1_raw": "Resistivo 1",
    "resistivo2_raw": "Resistivo 2",
    "resistivo3_raw": "Resistivo 3",
}

SENSOR_LABELS["capacitativo3_raw"] = "Capacitivo 3"

CSV_COLUMNS = BASE_COLUMNS + SENSOR_COLUMNS


def format_duration(seconds):
    """Formata segundos em HH:MM:SS. Aceita None (retorna '—')."""
    if seconds is None:
        return "—"
    seconds = max(0, int(round(seconds)))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


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
        self.csv_fieldnames = list(CSV_COLUMNS)
        self.experiment_dir = None
        self.metadata = None
        self.last_values = None
        self.last_timestamp = None
        self.last_error = None
        self.total_received = 0
        self.total_saved = 0
        self.history = deque(maxlen=600)

        # Controle do experimento
        self.duration_seconds = None
        self.interval_seconds = None
        self.target_samples = None
        self.waiting_first_sample = False
        self.t0 = None
        self._last_recorded_at = None
        self._last_esp_ms = None
        self._interval_deltas = []
        self._failsafe_deadline = None
        self._failsafe_remaining = None
        self.sensors_selected = list(SENSOR_COLUMNS)
        self._timer = None

    def connect(self, port, baud=115200):
        with self.lock:
            if self.connected:
                return

            try:
                conn = serial.Serial(
                    port,
                    baudrate=baud,
                    timeout=0.25,
                    exclusive=True,
                )
            except serial.SerialException as exc:
                raise RuntimeError(
                    f"Porta {port} ocupada ou indisponível ({exc}). "
                    "Feche o Monitor Serial do PlatformIO e outras conexões; "
                    "se persistir, reinicie o Streamlit."
                ) from exc

            conn.reset_input_buffer()

            self.serial_conn = conn
            self.connected = True
            self.running = True
            self.last_error = None

            self.thread = threading.Thread(
                target=self._reader_loop,
                args=(conn,),
                daemon=True,
            )
            self.thread.start()

    def disconnect(self):
        with self.lock:
            self.running = False
            self.recording = False
            self.paused = False
            self._close_experiment_locked("interrompido")

            self.connected = False
            thread = self.thread
            self.thread = None

        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)

        with self.lock:
            self._close_port_locked()

    def _close_port_locked(self):
        conn = self.serial_conn
        self.serial_conn = None
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def _reader_loop(self, conn):
        while True:
            with self.lock:
                if not self.running or self.serial_conn is not conn:
                    break

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
                        if self.waiting_first_sample:
                            self.waiting_first_sample = False
                            self.t0 = received_at
                            self._last_esp_ms = None

                            failsafe_seconds = self.duration_seconds * 2 + 30
                            self._failsafe_deadline = (
                                received_at + timedelta(seconds=failsafe_seconds)
                            )
                            self._schedule_timer(failsafe_seconds)

                            if self.metadata is not None:
                                self.metadata["status"] = "em_andamento"
                                self.metadata["t0_primeira_amostra"] = (
                                    received_at.isoformat(timespec="seconds")
                                )
                                self._write_metadata_locked()

                        esp_ms = values[0]
                        if self._last_esp_ms is not None:
                            delta = esp_ms - self._last_esp_ms
                            if delta > 0:
                                self._interval_deltas.append(delta)
                        self._last_esp_ms = esp_ms

                        self.csv_writer.writerow(row)
                        self.file_handle.flush()
                        self.total_saved += 1
                        self._last_recorded_at = received_at

                        if (
                            self.target_samples is not None
                            and self.total_saved >= self.target_samples
                        ):
                            self._close_experiment_locked(
                                "finalizado_por_amostras"
                            )

            except (ValueError, UnicodeDecodeError):
                continue

            except Exception as exc:
                with self.lock:
                    if self.running and self.serial_conn is conn:
                        self.last_error = str(exc)
                        self.running = False
                        self.connected = False
                        self._close_port_locked()
                break

    def _schedule_timer(self, seconds):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if seconds is None or seconds <= 0:
            return
        timer = threading.Timer(seconds, self._on_timer_expire)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def _on_timer_expire(self):
        with self.lock:
            self._timer = None
            if not self.recording or self.paused:
                return
            self._close_experiment_locked("finalizado_por_failsafe")

    def start_experiment(self, name, description, duration_seconds,
                         sensors_selected, interval_seconds):
        with self.lock:
            if not self.connected:
                raise RuntimeError("Conecte o ESP32 antes de iniciar.")
            if self.recording:
                raise RuntimeError("Ja existe um experimento em andamento.")
            if duration_seconds is None or duration_seconds <= 0:
                raise RuntimeError(
                    "A duração do experimento deve ser maior que zero."
                )
            if interval_seconds is None or interval_seconds <= 0:
                raise RuntimeError(
                    "O intervalo do ESP32 deve ser maior que zero."
                )

            selected = [
                s for s in SENSOR_COLUMNS
                if s in (sensors_selected or [])
            ]
            if not selected:
                raise RuntimeError(
                    "Selecione pelo menos um sensor para gravar."
                )

            target_samples = max(
                1, int(duration_seconds / interval_seconds + 0.5)
            )

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
            self.csv_fieldnames = BASE_COLUMNS + selected
            self.csv_writer = csv.DictWriter(
                self.file_handle,
                fieldnames=self.csv_fieldnames,
                extrasaction="ignore",
            )
            self.csv_writer.writeheader()
            self.file_handle.flush()
            self.experiment_dir = folder

            self.metadata = {
                "nome": name.strip() or "Experimento",
                "descricao": description.strip(),
                "inicio": now.isoformat(timespec="seconds"),
                "fim": None,
                "t0_primeira_amostra": None,
                "tN_ultima_amostra": None,
                "duracao_configurada_segundos": int(duration_seconds),
                "duracao_real_segundos": None,
                "intervalo_configurado_segundos": float(interval_seconds),
                "amostras_alvo": int(target_samples),
                "amostras_gravadas": 0,
                "intervalo_medido_ms_media": None,
                "intervalo_medido_ms_dp": None,
                "intervalo_medido_ms_min": None,
                "intervalo_medido_ms_max": None,
                "intervalo_medido_deltas_n": 0,
                "status": "aguardando_primeira_amostra",
                "porta_serial": self.serial_conn.port,
                "baud_rate": 115200,
                "sensores_selecionados": list(selected),
                "sensores_disponiveis": list(SENSOR_COLUMNS),
                "observacao": (
                    "Valores Raw originais do ADS1115; "
                    "calibracao ainda nao aplicada. "
                    "Criterio de parada: contagem de amostras "
                    "(duracao / intervalo)."
                ),
            }
            self._write_metadata_locked()

            self.recording = True
            self.paused = False
            self.total_saved = 0
            self.sensors_selected = selected
            self.duration_seconds = duration_seconds
            self.interval_seconds = interval_seconds
            self.target_samples = target_samples
            self.waiting_first_sample = True
            self.t0 = None
            self._last_recorded_at = None
            self._last_esp_ms = None
            self._interval_deltas = []
            self._failsafe_deadline = None
            self._failsafe_remaining = None

            return folder

    def pause(self):
        with self.lock:
            if self.recording and not self.paused:
                self.paused = True

                if self._timer is not None:
                    self._timer.cancel()
                    self._timer = None

                if self._failsafe_deadline is not None:
                    self._failsafe_remaining = max(
                        0.0,
                        (
                            self._failsafe_deadline
                            - datetime.now().astimezone()
                        ).total_seconds(),
                    )

                self.metadata["status"] = "pausado"
                self._write_metadata_locked()

    def resume(self):
        with self.lock:
            if self.recording and self.paused:
                self.paused = False

                if not self.waiting_first_sample:
                    budget = self._failsafe_remaining
                    if budget is None or budget <= 0:
                        budget = self.duration_seconds * 2 + 30
                    self._failsafe_deadline = (
                        datetime.now().astimezone()
                        + timedelta(seconds=budget)
                    )
                    self._schedule_timer(budget)

                self._failsafe_remaining = None

                if self.waiting_first_sample:
                    self.metadata["status"] = "aguardando_primeira_amostra"
                else:
                    self.metadata["status"] = "em_andamento"

                self._write_metadata_locked()

    def finish(self):
        with self.lock:
            if self.recording:
                self._close_experiment_locked("finalizado")

    def discard_experiment(self):
        with self.lock:
            folder = self.experiment_dir
            if not folder or not folder.exists():
                raise RuntimeError(
                    "Não há experimento disponível para descartar."
                )
            self._close_experiment_locked("descartado")
            shutil.rmtree(folder)
            self.experiment_dir = None
            self.metadata = None
            self.file_handle = None
            self.csv_writer = None

    def clear_visual_history(self):
        with self.lock:
            if self.recording:
                raise RuntimeError(
                    "Não é possível limpar os gráficos durante um experimento."
                )
            self.history.clear()

    def _write_metadata_locked(self):
        if self.experiment_dir and self.metadata:
            path = self.experiment_dir / "metadados.json"
            temp_path = self.experiment_dir / "metadados.tmp"
            temp_path.write_text(
                json.dumps(self.metadata, ensure_ascii=False, indent=4),
                encoding="utf-8",
            )
            temp_path.replace(path)

    def _close_experiment_locked(self, final_status):
        if self._timer is not None:
            try:
                self._timer.cancel()
            except Exception:
                pass
            self._timer = None

        if self.file_handle:
            try:
                self.file_handle.flush()
                self.file_handle.close()
            except Exception:
                pass

        self.file_handle = None
        self.csv_writer = None

        if self.metadata and self.experiment_dir:
            end_now = datetime.now().astimezone()
            self.metadata["fim"] = end_now.isoformat(timespec="seconds")
            self.metadata["status"] = final_status
            self.metadata["amostras_gravadas"] = int(self.total_saved)

            if self._last_recorded_at is not None:
                self.metadata["tN_ultima_amostra"] = (
                    self._last_recorded_at.isoformat(timespec="seconds")
                )

            if self.t0 is not None and self._last_recorded_at is not None:
                real_duration = (
                    self._last_recorded_at - self.t0
                ).total_seconds()
                self.metadata["duracao_real_segundos"] = round(real_duration, 3)

            deltas = self._interval_deltas
            if deltas:
                n = len(deltas)
                mean = sum(deltas) / n
                if n > 1:
                    var = sum((d - mean) ** 2 for d in deltas) / (n - 1)
                    std = var ** 0.5
                else:
                    std = 0.0
                self.metadata["intervalo_medido_ms_media"] = round(mean, 2)
                self.metadata["intervalo_medido_ms_dp"] = round(std, 2)
                self.metadata["intervalo_medido_ms_min"] = int(min(deltas))
                self.metadata["intervalo_medido_ms_max"] = int(max(deltas))
                self.metadata["intervalo_medido_deltas_n"] = n

            self._write_metadata_locked()

        self.recording = False
        self.paused = False
        self.waiting_first_sample = False
        self._failsafe_deadline = None
        self._failsafe_remaining = None

    def snapshot(self):
        with self.lock:
            samples_remaining = None
            time_remaining_estimate = None

            if self.recording:
                if self.target_samples is not None:
                    samples_remaining = max(
                        0, self.target_samples - self.total_saved
                    )
                    if self.interval_seconds:
                        time_remaining_estimate = (
                            samples_remaining * self.interval_seconds
                        )

            return {
                "connected": self.connected,
                "recording": self.recording,
                "paused": self.paused,
                "waiting_first_sample": self.waiting_first_sample,
                "last_values": self.last_values,
                "history": list(self.history),
                "last_error": self.last_error,
                "total_received": self.total_received,
                "total_saved": self.total_saved,
                "target_samples": self.target_samples,
                "samples_remaining": samples_remaining,
                "experiment_dir": (
                    str(self.experiment_dir)
                    if self.experiment_dir else None
                ),
                "metadata": (
                    dict(self.metadata) if self.metadata else None
                ),
                "time_remaining_seconds": time_remaining_estimate,
                "duration_seconds": self.duration_seconds,
                "interval_seconds": self.interval_seconds,
                "sensors_selected": list(self.sensors_selected),
            }


# ============================================================
# CONFIGURACAO DA INTERFACE
# ============================================================

st.set_page_config(
    page_title="Monitoramento de umidade",
    page_icon="🌱",
    layout="wide",
)

st.title("Monitoramento de umidade do solo")


@st.cache_resource
def get_collector():
    instance = SerialCollector()
    atexit.register(instance.disconnect)
    return instance


collector = get_collector()


def _rerun_app():
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


@st.fragment(run_every=1)
def sidebar_countdown_fragment():
    state = collector.snapshot()

    if not state["recording"]:
        _rerun_app()
        return

    target = state.get("target_samples")
    saved = state.get("total_saved", 0)
    estimated = state.get("time_remaining_seconds")

    if state.get("waiting_first_sample"):
        st.metric(
            "⏳ Aguardando 1ª leitura...",
            format_duration(state.get("duration_seconds")),
        )
        st.caption(
            "O cronômetro começa quando chegar a primeira amostra válida."
        )
        if target:
            st.caption(f"Amostras alvo: **{target}**")

    elif state["paused"]:
        st.metric(
            "⏸️ Tempo restante (estimado, pausado)",
            format_duration(estimated),
        )
        if target:
            st.caption(f"Amostras: **{saved} / {target}**")

    else:
        st.metric(
            "⏱️ Tempo restante (estimado)",
            format_duration(estimated),
        )
        if target:
            st.caption(f"Amostras: **{saved} / {target}**")


# ============================================================
# BARRA LATERAL
# ============================================================

with st.sidebar:

    st.header("Conexão")

    ports = list(serial.tools.list_ports.comports())
    port_options = [p.device for p in ports]

    if not port_options:
        port_options = ["/dev/ttyUSB0", "/dev/ttyACM0"]

    selected_port = st.selectbox("Porta serial", port_options)

    if not collector.snapshot()["connected"]:
        if st.button(
            "Conectar ESP32", type="primary", width="stretch"
        ):
            try:
                collector.connect(selected_port, 115200)
                st.success(f"Conectado em {selected_port}")
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao conectar: {exc}")
    else:
        st.success("ESP32 conectado")
        if st.button("Desconectar", width="stretch"):
            collector.disconnect()
            st.rerun()

    st.divider()

    # --------------------------------------------------------
    # NOVO EXPERIMENTO
    # --------------------------------------------------------

    st.header("Novo experimento")

    exp_name = st.text_input(
        "Nome", value="Monitoramento do solo"
    )

    exp_description = st.text_area(
        "Descrição",
        placeholder="Descreva as condições do experimento...",
    )

    st.markdown("**Duração de referência**")

    dur_cols = st.columns(3)

    dur_h = dur_cols[0].number_input(
        "Horas", min_value=0, max_value=99,
        value=0, step=1, key="exp_dur_h",
    )
    dur_m = dur_cols[1].number_input(
        "Minutos", min_value=0, max_value=59,
        value=0, step=1, key="exp_dur_m",
    )
    dur_s = dur_cols[2].number_input(
        "Segundos", min_value=0, max_value=59,
        value=10, step=1, key="exp_dur_s",
    )

    duration_seconds = dur_h * 3600 + dur_m * 60 + dur_s

    interval = st.number_input(
        "Intervalo configurado no ESP32 (segundos)",
        min_value=1, value=2, step=1,
    )

    st.markdown("**Sensores a gravar no CSV**")

    sensors_selected = []
    for sensor_key in SENSOR_COLUMNS:
        if st.checkbox(
            SENSOR_LABELS[sensor_key],
            value=True,
            key=f"sensor_cb_{sensor_key}",
        ):
            sensors_selected.append(sensor_key)

    if not sensors_selected:
        st.warning(
            "Marque pelo menos um sensor para poder iniciar o experimento."
        )

    # --------------------------------------------------------
    # BOTAO DE INICIAR **OU** CONTAGEM REGRESSIVA
    # --------------------------------------------------------

    if collector.snapshot()["recording"]:
        sidebar_countdown_fragment()
    else:
        if st.button(
            "Iniciar experimento", type="primary", width="stretch"
        ):
            try:
                if duration_seconds <= 0:
                    st.error(
                        "Defina uma duração maior que zero (h/m/s)."
                    )
                elif not sensors_selected:
                    st.error(
                        "Selecione pelo menos um sensor para gravar."
                    )
                elif interval <= 0:
                    st.error(
                        "O intervalo do ESP32 deve ser maior que zero."
                    )
                else:
                    folder = collector.start_experiment(
                        exp_name,
                        exp_description,
                        duration_seconds,
                        sensors_selected,
                        interval,
                    )
                    st.success(f"Gravando em: {folder}")
                    st.rerun()
            except Exception as exc:
                st.error(str(exc))

    # --------------------------------------------------------
    # CONTROLE DA GRAVACAO
    # --------------------------------------------------------

    state = collector.snapshot()

    if state["recording"]:
        if state["paused"]:
            if st.button("Retomar gravação", width="stretch"):
                collector.resume()
                st.rerun()
        else:
            if st.button("Pausar gravação", width="stretch"):
                collector.pause()
                st.rerun()

        if st.button(
            "Finalizar experimento",
            type="secondary",
            width="stretch",
        ):
            collector.finish()
            st.success(
                "Experimento finalizado e arquivos fechados."
            )
            st.rerun()

    # --------------------------------------------------------
    # LIMPEZA VISUAL DOS GRAFICOS
    # --------------------------------------------------------

    st.divider()
    st.header("Gráficos")

    state = collector.snapshot()

    if state["recording"]:
        st.button(
            "Limpar gráficos",
            disabled=True,
            width="stretch",
            help="Finalize o experimento antes de limpar os gráficos.",
        )
        st.caption(
            "A limpeza está disponível somente quando não há gravação ativa."
        )
    else:
        if st.button("Limpar gráficos", width="stretch"):
            try:
                collector.clear_visual_history()
                st.success("Histórico visual dos gráficos limpo.")
                st.rerun()
            except Exception as exc:
                st.error(
                    f"Não foi possível limpar os gráficos: {exc}"
                )

    # --------------------------------------------------------
    # DESCARTAR EXPERIMENTO
    # --------------------------------------------------------

    state = collector.snapshot()

    if state["experiment_dir"]:
        if st.button("Descartar experimento", width="stretch"):
            st.session_state["confirm_discard_experiment"] = True

        if st.session_state.get("confirm_discard_experiment", False):
            st.warning(
                "Isso apagará permanentemente a pasta do experimento, "
                "incluindo dados.csv e metadados.json."
            )

            confirm_col, cancel_col = st.columns(2)

            if confirm_col.button(
                "Confirmar descarte",
                type="primary",
                width="stretch",
            ):
                try:
                    collector.discard_experiment()
                    st.session_state["confirm_discard_experiment"] = False
                    st.success("Experimento descartado.")
                    st.rerun()
                except Exception as exc:
                    st.error(
                        f"Não foi possível descartar: {exc}"
                    )

            if cancel_col.button("Cancelar", width="stretch"):
                st.session_state["confirm_discard_experiment"] = False
                st.rerun()


# ============================================================
# DASHBOARD EM TEMPO REAL
# ============================================================

@st.fragment(run_every=1)
def live_dashboard():

    data = collector.snapshot()

    if data["last_error"]:
        st.error(
            f"Erro na comunicação serial: {data['last_error']}"
        )

    if not data["connected"]:
        st.warning("ESP32 desconectado")

    if data["recording"] and data.get("waiting_first_sample"):
        if data["paused"]:
            st.info(
                "Experimento pausado durante a espera pela primeira "
                "amostra. O cronômetro ainda não iniciou."
            )
        else:
            st.info(
                "Experimento iniciado. Aguardando a primeira leitura "
                "válida do ESP32 para começar a contar."
            )
    elif data["recording"] and data["paused"]:
        st.info(
            "Experimento pausado: os dados continuam sendo recebidos, "
            "mas não são gravados."
        )
    elif data["recording"]:
        st.success("Experimento em gravação")

    # --------------------------------------------------------
    # PROGRESSO DA GRAVACAO
    # --------------------------------------------------------

    if data["recording"] and data.get("target_samples"):
        target = data["target_samples"]
        saved = data["total_saved"]
        progress = min(1.0, saved / target) if target > 0 else 0.0
        st.progress(progress, text=f"Amostras gravadas: {saved} / {target}")

    # --------------------------------------------------------
    # LEITURAS ATUAIS
    # --------------------------------------------------------

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

        st.caption(
            f"Última leitura recebida: {values['timestamp']}"
        )
    else:
        st.info(
            "Aguardando a primeira linha válida enviada pelo ESP32..."
        )

    # --------------------------------------------------------
    # GRAFICOS EM TEMPO REAL
    # --------------------------------------------------------

    st.subheader("Gráficos em tempo real")

    history = data["history"]

    if history:
        df = pd.DataFrame(history)
        df["amostra"] = range(1, len(df) + 1)

        capacitive_columns = [
            "capacitativo1_raw",
            "capacitativo2_raw",
            "capacitativo3_raw",
        ]
        resistive_columns = [
            "resistivo1_raw",
            "resistivo2_raw",
            "resistivo3_raw",
        ]

        st.markdown("**Sensores capacitivos**")

        fig_cap = px.line(
            df,
            x="amostra",
            y=capacitive_columns,
            labels={
                "amostra": "Número da amostra",
                "value": "Valor Raw",
                "variable": "Sensor",
            },
        )
        fig_cap.for_each_trace(
            lambda trace: trace.update(
                name=SENSOR_LABELS.get(trace.name, trace.name)
            )
        )
        fig_cap.update_layout(
            yaxis_title="Valor Raw",
            legend_title="Sensor",
        )
        st.plotly_chart(fig_cap, width="stretch")

        st.markdown("**Sensores resistivos**")

        fig_res = px.line(
            df,
            x="amostra",
            y=resistive_columns,
            labels={
                "amostra": "Número da amostra",
                "value": "Valor Raw",
                "variable": "Sensor",
            },
        )
        fig_res.for_each_trace(
            lambda trace: trace.update(
                name=SENSOR_LABELS.get(trace.name, trace.name)
            )
        )
        fig_res.update_layout(
            yaxis_title="Valor Raw",
            legend_title="Sensor",
        )
        st.plotly_chart(fig_res, width="stretch")
    else:
        st.info(
            "Os gráficos aparecerão assim que chegarem leituras válidas."
        )

    # --------------------------------------------------------
    # ESTADO DA GRAVACAO
    # --------------------------------------------------------

    st.subheader("Estado da gravação")

    c1, c2, c3 = st.columns(3)

    c1.metric("Linhas recebidas", data["total_received"])
    c2.metric("Linhas gravadas", data["total_saved"])
    c3.metric(
        "Amostras alvo",
        data["target_samples"] if data.get("target_samples") else "—",
    )

    if data["recording"] and data["sensors_selected"]:
        st.caption(
            "Sensores sendo gravados no CSV: "
            + ", ".join(
                SENSOR_LABELS[s] for s in data["sensors_selected"]
            )
        )

    if data["experiment_dir"]:
        st.code(data["experiment_dir"], language=None)


# ============================================================
# ANALISE DE EXPERIMENTOS SALVOS
# ============================================================

def _list_experiments():
    if not EXPERIMENTS_DIR.exists():
        return []
    result = [
        d for d in EXPERIMENTS_DIR.iterdir()
        if d.is_dir() and (d / "dados.csv").exists()
    ]
    result.sort(key=lambda p: p.name, reverse=True)
    return result


def _load_experiment(folder):
    df = pd.read_csv(folder / "dados.csv")
    meta = None
    meta_path = folder / "metadados.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = None
    return df, meta


def _format_experiment_label(folder, meta=None):
    if meta is None:
        meta_path = folder / "metadados.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = None

    if meta:
        nome = meta.get("nome") or folder.name
        inicio = meta.get("inicio")
        if inicio:
            try:
                dt = datetime.fromisoformat(inicio)
                return f"{dt.strftime('%d/%m/%Y %H:%M:%S')} — {nome}"
            except Exception:
                pass
        return nome
    return folder.name


def _sensors_in_df(df):
    return [c for c in SENSOR_COLUMNS if c in df.columns]


def _x_axis_label(x):
    return {
        "tempo_esp_ms": "Tempo no ESP32 (ms)",
        "timestamp": "Data/hora",
        "amostra": "Número da amostra",
    }.get(x, x)


def _plot_line(df, x, columns, title=None):
    if not columns:
        return None
    fig = px.line(df, x=x, y=columns)
    fig.for_each_trace(
        lambda t: t.update(name=SENSOR_LABELS.get(t.name, t.name))
    )
    fig.update_layout(
        title=title,
        yaxis_title="Valor Raw",
        legend_title="Sensor",
        xaxis_title=_x_axis_label(x),
        uirevision="constante",
        margin=dict(l=10, r=10, t=40 if title else 10, b=10),
    )
    return fig


def render_analysis_tab():
    st.header("Análise de experimentos salvos")

    experiments = _list_experiments()

    if not experiments:
        st.info(
            "Nenhum experimento encontrado. "
            "Inicie um experimento na aba "
            "\"Monitoramento ao vivo\" para gerar arquivos."
        )
        return

    folder_names = [p.name for p in experiments]
    labels_map = {
        p.name: _format_experiment_label(p)
        for p in experiments
    }

    selected_folder_name = st.selectbox(
        "Experimento",
        folder_names,
        format_func=lambda n: labels_map.get(n, n),
        key="analysis_selected_folder",
    )

    folder = EXPERIMENTS_DIR / selected_folder_name
    if not folder.exists():
        st.warning(
            "A pasta selecionada não existe mais. "
            "Selecione outro experimento."
        )
        return

    try:
        df, meta = _load_experiment(folder)
    except Exception as exc:
        st.error(f"Não foi possível ler o experimento: {exc}")
        return

    if df.empty:
        st.warning("O CSV está vazio.")
        return

    st.code(str(folder), language=None)

    # ---- Metadados ----
    if meta:
        with st.expander("📋 Metadados do experimento", expanded=False):
            mcol1, mcol2 = st.columns(2)
            mcol1.markdown(
                f"**Nome:** {meta.get('nome', '—')}\n\n"
                f"**Início:** {meta.get('inicio', '—')}\n\n"
                f"**Fim:** {meta.get('fim', '—')}\n\n"
                f"**Status:** {meta.get('status', '—')}\n\n"
                f"**t0 (1ª amostra):** "
                f"{meta.get('t0_primeira_amostra', '—')}\n\n"
                f"**tN (última amostra):** "
                f"{meta.get('tN_ultima_amostra', '—')}"
            )
            mcol2.markdown(
                f"**Duração configurada:** "
                f"{format_duration(meta.get('duracao_configurada_segundos'))}\n\n"
                f"**Duração real:** "
                f"{meta.get('duracao_real_segundos', '—')} s\n\n"
                f"**Amostras alvo:** "
                f"{meta.get('amostras_alvo', '—')}\n\n"
                f"**Amostras gravadas:** "
                f"{meta.get('amostras_gravadas', '—')}\n\n"
                f"**Intervalo configurado:** "
                f"{meta.get('intervalo_configurado_segundos', '—')} s\n\n"
                f"**Porta:** {meta.get('porta_serial', '—')}"
            )
            if meta.get("descricao"):
                st.markdown(f"**Descrição:** {meta['descricao']}")

            st.markdown("**Intervalo medido (tempo_esp_ms)**")
            ic1, ic2, ic3, ic4 = st.columns(4)
            ic1.metric(
                "Média (ms)",
                meta.get("intervalo_medido_ms_media", "—"),
            )
            ic2.metric(
                "Desvio-padrão (ms)",
                meta.get("intervalo_medido_ms_dp", "—"),
            )
            ic3.metric(
                "Mínimo (ms)",
                meta.get("intervalo_medido_ms_min", "—"),
            )
            ic4.metric(
                "Máximo (ms)",
                meta.get("intervalo_medido_ms_max", "—"),
            )

    # ---- Sensores disponiveis ----
    available_sensors = _sensors_in_df(df)
    if not available_sensors:
        st.warning(
            "O CSV não contém colunas de sensores reconhecidas."
        )
        return

    st.caption(
        f"{len(df):,} amostras · "
        f"colunas: {', '.join(df.columns.tolist())}"
    )

    df = df.copy()
    df["amostra"] = range(1, len(df) + 1)

    x_candidates = []
    if "tempo_esp_ms" in df.columns:
        x_candidates.append("tempo_esp_ms")
    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(
                df["timestamp"], errors="coerce"
            )
            if df["timestamp"].notna().any():
                x_candidates.append("timestamp")
        except Exception:
            pass
    x_candidates.append("amostra")

    ctrl_mode, ctrl_x, ctrl_sensors = st.columns([1.2, 1, 1.6])

    mode = ctrl_mode.radio(
        "Modo de visualização",
        [
            "Todos os sensores juntos",
            "Por tipo (capacitivo/resistivo)",
            "Um gráfico por sensor",
        ],
        key=f"analysis_mode_{selected_folder_name}",
    )

    x_axis = ctrl_x.selectbox(
        "Eixo X",
        x_candidates,
        format_func=_x_axis_label,
        key=f"analysis_x_axis_{selected_folder_name}",
    )

    selected_sensors = ctrl_sensors.multiselect(
        "Sensores",
        options=available_sensors,
        default=available_sensors,
        format_func=lambda s: SENSOR_LABELS.get(s, s),
        key=f"analysis_sensors_{selected_folder_name}",
    )

    if not selected_sensors:
        st.warning("Selecione pelo menos um sensor.")
        return

    with st.expander("⚙️ Opções avançadas", expanded=False):
        max_points = st.number_input(
            "Máximo de pontos por gráfico (0 = todos)",
            min_value=0,
            value=0,
            step=500,
            key=f"analysis_max_points_{selected_folder_name}",
        )

    df_plot = df
    if max_points and len(df_plot) > max_points:
        step = max(1, len(df_plot) // int(max_points))
        df_plot = df_plot.iloc[::step].reset_index(drop=True)
        st.caption(
            f"Exibindo {len(df_plot):,} de {len(df):,} amostras "
            f"(1 a cada {step} pontos)."
        )

    with st.expander("📊 Estatísticas dos sensores", expanded=False):
        stats_rows = []
        for col in selected_sensors:
            s = df[col]
            stats_rows.append({
                "Sensor": SENSOR_LABELS.get(col, col),
                "Mínimo": float(s.min()),
                "Máximo": float(s.max()),
                "Média": float(s.mean()),
                "Desvio": float(s.std()) if len(s) > 1 else 0.0,
            })
        st.dataframe(
            pd.DataFrame(stats_rows),
            width="stretch",
            hide_index=True,
        )

    st.subheader("Gráficos")

    if mode == "Todos os sensores juntos":
        fig = _plot_line(df_plot, x_axis, selected_sensors)
        st.plotly_chart(fig, width="stretch")

    elif mode == "Por tipo (capacitivo/resistivo)":
        cap_cols = [
            c for c in selected_sensors if c.startswith("capacitativo")
        ]
        res_cols = [
            c for c in selected_sensors if c.startswith("resistivo")
        ]

        if cap_cols:
            st.markdown("**Sensores capacitivos**")
            st.plotly_chart(
                _plot_line(df_plot, x_axis, cap_cols),
                width="stretch",
            )
        if res_cols:
            st.markdown("**Sensores resistivos**")
            st.plotly_chart(
                _plot_line(df_plot, x_axis, res_cols),
                width="stretch",
            )
    else:
        cols = st.columns(2)
        for i, sensor in enumerate(selected_sensors):
            with cols[i % 2]:
                fig = _plot_line(
                    df_plot, x_axis, [sensor],
                    title=SENSOR_LABELS.get(sensor, sensor),
                )
                st.plotly_chart(
                    fig,
                    width="stretch",
                    key=(
                        f"analysis_plot_{selected_folder_name}_{sensor}"
                    ),
                )


# ============================================================
# ABAS PRINCIPAIS
# ============================================================

tab_live, tab_analysis = st.tabs([
    "📈 Monitoramento ao vivo",
    "🗂️ Análise de experimentos",
])

with tab_live:
    live_dashboard()

with tab_analysis:
    render_analysis_tab()