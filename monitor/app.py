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
    "capacitativo2_raw": "Capacitivo 2",
    "capacitativo3_raw": "Capacitivo 3",
    "resistivo1_raw": "Resistivo 1",
    "resistivo2_raw": "Resistivo 2",
    "resistivo3_raw": "Resistivo 3",
}

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

        # Controle de duracao
        self.end_time = None
        self.paused_remaining = None
        self.duration_seconds = None
        self.sensors_selected = list(SENSOR_COLUMNS)
        self._timer = None

    def connect(self, port, baud=115200):
        with self.lock:
            if self.connected:
                return

            # Impede conexoes simultaneas na mesma porta.
            try:
                conn = serial.Serial(
                    port,
                    baudrate=baud,
                    timeout=0.25,
                    exclusive=True
                )
            except serial.SerialException as exc:
                raise RuntimeError(
                    f"Porta {port} ocupada ou indisponível ({exc}). "
                    "Feche o Monitor Serial do PlatformIO e outras conexões; "
                    "se persistir, reinicie o Streamlit."
                ) from exc

            # Descarta bytes antigos do buffer.
            conn.reset_input_buffer()

            self.serial_conn = conn
            self.connected = True
            self.running = True
            self.last_error = None

            self.thread = threading.Thread(
                target=self._reader_loop,
                args=(conn,),
                daemon=True
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

        # Espera a thread terminar antes de fechar a porta.
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
                # Encerra se desconectou ou se esta thread ficou obsoleta.
                if not self.running or self.serial_conn is not conn:
                    break

            try:
                raw_line = conn.readline()

                if not raw_line:
                    continue

                line = raw_line.decode(
                    "utf-8",
                    errors="ignore"
                ).strip()

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

                    # Historico utilizado exclusivamente pelos graficos.
                    self.history.append(row)

                    # Gravacao CSV independente do historico visual.
                    # extrasaction="ignore" faz o DictWriter descartar
                    # automaticamente as colunas nao selecionadas.
                    if self.recording and not self.paused and self.csv_writer:
                        self.csv_writer.writerow(row)
                        self.file_handle.flush()
                        self.total_saved += 1

            except (ValueError, UnicodeDecodeError):
                # Ignora linhas invalidas enviadas pelo ESP32.
                continue

            except Exception as exc:
                with self.lock:
                    if self.running and self.serial_conn is conn:
                        self.last_error = str(exc)
                        self.running = False
                        self.connected = False

                        # Libera a porta para permitir reconexao.
                        self._close_port_locked()

                break

    def _schedule_timer(self, seconds):
        """Agenda a finalizacao automatica do experimento."""
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
            # Se o usuario pausou exatamente agora, nao finaliza.
            if not self.recording or self.paused:
                return
            self._close_experiment_locked("finalizado_por_tempo")

    def start_experiment(self, name, description, duration_seconds, sensors_selected):
        with self.lock:
            if not self.connected:
                raise RuntimeError("Conecte o ESP32 antes de iniciar.")

            if self.recording:
                raise RuntimeError("Ja existe um experimento em andamento.")

            if duration_seconds is None or duration_seconds <= 0:
                raise RuntimeError(
                    "A duração do experimento deve ser maior que zero."
                )

            # Filtra a selecao pelos nomes canonicos conhecidos.
            selected = [
                s for s in SENSOR_COLUMNS
                if s in (sensors_selected or [])
            ]
            if not selected:
                raise RuntimeError(
                    "Selecione pelo menos um sensor para gravar."
                )

            now = datetime.now().astimezone()

            safe_name = "".join(
                ch if ch.isalnum() or ch in "-_" else "_"
                for ch in name.strip()
            ).strip("_")

            if not safe_name:
                safe_name = "experimento"

            folder_name = (
                now.strftime("%Y%m%d_%H%M%S")
                + "_"
                + safe_name
            )

            folder = EXPERIMENTS_DIR / folder_name
            folder.mkdir(parents=True, exist_ok=False)

            csv_path = folder / "dados.csv"

            self.file_handle = csv_path.open(
                "w",
                newline="",
                encoding="utf-8"
            )

            # Header dinamico: base + apenas os sensores selecionados.
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
                "status": "em_andamento",
                "porta_serial": self.serial_conn.port,
                "baud_rate": 115200,
                "intervalo_configurado_esp_ms": None,
                "intervalo_exibido_segundos": None,
                "duracao_configurada_segundos": int(duration_seconds),
                "sensores_selecionados": list(selected),
                "sensores_disponiveis": list(SENSOR_COLUMNS),
                "observacao": (
                    "Valores Raw originais do ADS1115; "
                    "calibracao ainda nao aplicada."
                )
            }

            self._write_metadata_locked()

            self.recording = True
            self.paused = False
            self.total_saved = 0
            self.sensors_selected = selected
            self.duration_seconds = duration_seconds
            self.end_time = now + timedelta(seconds=duration_seconds)
            self.paused_remaining = None

            self._schedule_timer(duration_seconds)

            return folder

    def pause(self):
        with self.lock:
            if self.recording and not self.paused:
                self.paused = True

                # Congela o cronometro: guarda o quanto falta e cancela o timer.
                if self._timer is not None:
                    self._timer.cancel()
                    self._timer = None

                if self.end_time is not None:
                    self.paused_remaining = max(
                        0.0,
                        (
                            self.end_time - datetime.now().astimezone()
                        ).total_seconds()
                    )

                self.metadata["status"] = "pausado"
                self._write_metadata_locked()

    def resume(self):
        with self.lock:
            if self.recording and self.paused:
                self.paused = False

                if self.paused_remaining is not None:
                    if self.paused_remaining > 0:
                        self.end_time = (
                            datetime.now().astimezone()
                            + timedelta(seconds=self.paused_remaining)
                        )
                        self._schedule_timer(self.paused_remaining)
                    else:
                        # Nao havia tempo restante: finaliza imediatamente.
                        self.paused_remaining = None
                        self._close_experiment_locked("finalizado_por_tempo")
                        return

                self.paused_remaining = None
                self.metadata["status"] = "em_andamento"
                self._write_metadata_locked()

    def finish(self):
        with self.lock:
            self._close_experiment_locked("finalizado")

    def discard_experiment(self):
        """Fecha e apaga permanentemente a pasta do experimento atual."""
        with self.lock:
            folder = self.experiment_dir

            if not folder or not folder.exists():
                raise RuntimeError(
                    "Não há experimento disponível para descartar."
                )

            # Fecha o CSV antes de remover a pasta.
            self._close_experiment_locked("descartado")

            shutil.rmtree(folder)

            self.experiment_dir = None
            self.metadata = None
            self.file_handle = None
            self.csv_writer = None

    def clear_visual_history(self):
        """
        Limpa somente o historico utilizado pelos graficos.

        Nao interfere na conexao serial, nas leituras atuais,
        nos contadores ou nos arquivos CSV.
        """
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
                json.dumps(
                    self.metadata,
                    ensure_ascii=False,
                    indent=4
                ),
                encoding="utf-8"
            )

            temp_path.replace(path)

    def _close_experiment_locked(self, final_status):
        # Cancela o timer caso ainda exista.
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
            self.metadata["fim"] = (
                datetime.now()
                .astimezone()
                .isoformat(timespec="seconds")
            )

            self.metadata["status"] = final_status
            self._write_metadata_locked()

        self.recording = False
        self.paused = False
        self.end_time = None
        self.paused_remaining = None

    def snapshot(self):
        with self.lock:
            time_remaining = None
            if self.recording:
                if self.paused and self.paused_remaining is not None:
                    time_remaining = self.paused_remaining
                elif not self.paused and self.end_time is not None:
                    time_remaining = max(
                        0.0,
                        (
                            self.end_time - datetime.now().astimezone()
                        ).total_seconds()
                    )

            return {
                "connected": self.connected,
                "recording": self.recording,
                "paused": self.paused,
                "last_values": self.last_values,
                "history": list(self.history),
                "last_error": self.last_error,
                "total_received": self.total_received,
                "total_saved": self.total_saved,
                "experiment_dir": (
                    str(self.experiment_dir)
                    if self.experiment_dir
                    else None
                ),
                "metadata": (
                    dict(self.metadata)
                    if self.metadata
                    else None
                ),
                "time_remaining_seconds": time_remaining,
                "duration_seconds": self.duration_seconds,
                "sensors_selected": list(self.sensors_selected),
            }


# ============================================================
# CONFIGURACAO DA INTERFACE
# ============================================================

st.set_page_config(
    page_title="Monitoramento de umidade",
    page_icon="🌱",
    layout="wide"
)

st.title("Monitoramento de umidade do solo")

st.caption(
    "ESP32 + dois ADS1115 | "
    "Aquisição serial, gravação CSV e gráficos em tempo real"
)


@st.cache_resource
def get_collector():
    """
    Um unico coletor para todo o processo do Streamlit.

    O refresh ou outra aba reutiliza a mesma conexao
    e o mesmo experimento em andamento.
    """
    instance = SerialCollector()

    # Fecha a porta e o CSV ao encerrar o Streamlit.
    atexit.register(instance.disconnect)

    return instance


collector = get_collector()


def _rerun_app():
    """
    Forca um rerun completo do app (nao apenas do fragmento atual).

    Necessario para que o botao "Iniciar experimento" volte a aparecer
    no sidebar quando o experimento terminar por tempo enquanto o usuario
    esta ocioso. Usa fallback para versoes antigas do Streamlit onde o
    parametro `scope` ainda nao existe (nessas versoes, st.rerun() ja
    rerenderizava o app inteiro).
    """
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


@st.fragment(run_every=1)
def sidebar_countdown_fragment():
    """
    Contagem regressiva do experimento em andamento, exibida no lugar
    do botao "Iniciar experimento" dentro do menu "Novo experimento".
    """
    state = collector.snapshot()

    if not state["recording"]:
        # O experimento terminou enquanto este fragmento estava ativo
        # (provavelmente por tempo, via timer em background). Forca um
        # rerun completo para que o botao "Iniciar experimento" volte
        # a aparecer no sidebar.
        _rerun_app()
        return

    remaining = state["time_remaining_seconds"]

    if state["paused"]:
        st.metric(
            "⏸️ Tempo restante (pausado)",
            format_duration(remaining),
        )
    else:
        st.metric(
            "⏱️ Tempo restante",
            format_duration(remaining),
        )

    if state.get("duration_seconds"):
        st.caption(
            f"Duração total: {format_duration(state['duration_seconds'])}"
        )


# ============================================================
# BARRA LATERAL
# ============================================================

with st.sidebar:

    st.header("Conexão")

    ports = list(serial.tools.list_ports.comports())
    port_options = [p.device for p in ports]

    if not port_options:
        port_options = [
            "/dev/ttyUSB0",
            "/dev/ttyACM0"
        ]

    selected_port = st.selectbox(
        "Porta serial",
        port_options
    )

    st.caption(
        "Feche o Monitor Serial do PlatformIO antes de conectar."
    )

    if not collector.snapshot()["connected"]:

        if st.button(
            "Conectar ESP32",
            type="primary",
            width="stretch",
        ):
            try:
                collector.connect(
                    selected_port,
                    115200
                )

                st.success(
                    f"Conectado em {selected_port}"
                )

                st.rerun()

            except Exception as exc:
                st.error(
                    f"Falha ao conectar: {exc}"
                )

    else:
        st.success("ESP32 conectado")

        if st.button(
            "Desconectar",
            width="stretch",
        ):
            collector.disconnect()
            st.rerun()

    st.divider()

    # --------------------------------------------------------
    # NOVO EXPERIMENTO
    # --------------------------------------------------------

    st.header("Novo experimento")

    exp_name = st.text_input(
        "Nome",
        value="Monitoramento do solo"
    )

    exp_description = st.text_area(
        "Descrição",
        placeholder="Descreva as condições do experimento..."
    )

    st.markdown("**Duração do experimento**")

    dur_cols = st.columns(3)

    dur_h = dur_cols[0].number_input(
        "Horas",
        min_value=0,
        max_value=99,
        value=0,
        step=1,
        key="exp_dur_h",
    )
    dur_m = dur_cols[1].number_input(
        "Minutos",
        min_value=0,
        max_value=59,
        value=0,
        step=1,
        key="exp_dur_m",
    )
    dur_s = dur_cols[2].number_input(
        "Segundos",
        min_value=0,
        max_value=59,
        value=10,
        step=1,
        key="exp_dur_s",
    )

    duration_seconds = dur_h * 3600 + dur_m * 60 + dur_s

    st.caption(
        f"Duração total: {format_duration(duration_seconds)}"
    )

    interval = st.number_input(
        "Intervalo configurado no ESP32 (segundos)",
        min_value=1,
        value=2,
        step=1
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
        # Experimento em andamento: substitui o botao pela contagem.
        sidebar_countdown_fragment()
    else:
        if st.button(
            "Iniciar experimento",
            type="primary",
            width="stretch",
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
                else:
                    folder = collector.start_experiment(
                        exp_name,
                        exp_description,
                        duration_seconds,
                        sensors_selected,
                    )

                    st.success(
                        f"Gravando em: {folder}"
                    )

                    st.rerun()

            except Exception as exc:
                st.error(str(exc))

    # --------------------------------------------------------
    # CONTROLE DA GRAVACAO
    # --------------------------------------------------------

    state = collector.snapshot()

    if state["recording"]:

        if state["paused"]:

            if st.button(
                "Retomar gravação",
                width="stretch",
            ):
                collector.resume()
                st.rerun()

        else:

            if st.button(
                "Pausar gravação",
                width="stretch",
            ):
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
            help=(
                "Finalize o experimento antes de limpar os gráficos."
            )
        )

        st.caption(
            "A limpeza está disponível somente quando "
            "não há gravação ativa."
        )

    else:

        if st.button(
            "Limpar gráficos",
            width="stretch",
        ):
            try:
                collector.clear_visual_history()

                st.success(
                    "Histórico visual dos gráficos limpo."
                )

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

        if st.button(
            "Descartar experimento",
            width="stretch",
        ):
            st.session_state[
                "confirm_discard_experiment"
            ] = True

        if st.session_state.get(
            "confirm_discard_experiment",
            False
        ):

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

                    st.session_state[
                        "confirm_discard_experiment"
                    ] = False

                    st.success("Experimento descartado.")

                    st.rerun()

                except Exception as exc:
                    st.error(
                        f"Não foi possível descartar: {exc}"
                    )

            if cancel_col.button(
                "Cancelar",
                width="stretch",
            ):
                st.session_state[
                    "confirm_discard_experiment"
                ] = False

                st.rerun()

    st.divider()

    st.caption(
        "Os arquivos ficam na pasta `experimentos/` deste projeto."
    )


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

    if data["connected"]:
        st.success("Comunicação serial ativa")
    else:
        st.warning("ESP32 desconectado")

    if data["recording"] and data["paused"]:

        st.info(
            "Experimento pausado: os dados continuam sendo "
            "recebidos, mas não são gravados. O cronômetro "
            "está congelado."
        )

    elif data["recording"]:

        st.success("Experimento em gravação")

    else:

        st.info(
            "Nenhum experimento está sendo gravado."
        )

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

            cols[idx % 3].metric(
                label,
                values[key]
            )

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

        df["amostra"] = range(
            1,
            len(df) + 1
        )

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

        # ----------------------------------------------------
        # SENSORES CAPACITIVOS
        # ----------------------------------------------------

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
                name=SENSOR_LABELS.get(
                    trace.name,
                    trace.name
                )
            )
        )

        fig_cap.update_layout(
            yaxis_title="Valor Raw",
            legend_title="Sensor"
        )

        st.plotly_chart(
            fig_cap,
            width="stretch",
        )

        # ----------------------------------------------------
        # SENSORES RESISTIVOS
        # ----------------------------------------------------

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
                name=SENSOR_LABELS.get(
                    trace.name,
                    trace.name
                )
            )
        )

        fig_res.update_layout(
            yaxis_title="Valor Raw",
            legend_title="Sensor"
        )

        st.plotly_chart(
            fig_res,
            width="stretch",
        )

    else:

        st.info(
            "Os gráficos aparecerão assim que chegarem leituras válidas."
        )

    # --------------------------------------------------------
    # ESTADO DA GRAVACAO
    # (contagem removida: agora só existe no sidebar)
    # --------------------------------------------------------

    st.subheader("Estado da gravação")

    c1, c2 = st.columns(2)

    c1.metric(
        "Linhas recebidas",
        data["total_received"]
    )

    c2.metric(
        "Linhas gravadas",
        data["total_saved"]
    )

    if data["recording"] and data["sensors_selected"]:
        st.caption(
            "Sensores sendo gravados no CSV: "
            + ", ".join(
                SENSOR_LABELS[s] for s in data["sensors_selected"]
            )
        )

    if data["experiment_dir"]:

        st.code(
            data["experiment_dir"],
            language=None
        )


live_dashboard()


# ============================================================
# RODAPE
# ============================================================

st.divider()

st.caption(
    "Os valores apresentados são Raw do ADS1115. "
    "Faça a calibração individual dos sensores antes de "
    "interpretar os valores como porcentagem de umidade."
)