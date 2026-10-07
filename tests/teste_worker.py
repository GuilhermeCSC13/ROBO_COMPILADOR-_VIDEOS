# -*- coding: utf-8 -*-
"""
Teste do worker SEM tocar no Supabase de verdade: banco e Storage são falsos (em memória) e o envio
TUS é simulado. O ffmpeg é real — as "partes" da gravação são geradas aqui.

Cenários:
  A) reunião longa: MP4 passa do limite → comprime; 1º envio do vídeo dá 500 → tenta de novo e sobe.
     Esperado: áudio sobe ANTES do vídeo, reunião CONCLUIDO, partes apagadas.
  B) vídeo impossível de caber: áudio sobe, vídeo não. Esperado: reunião ERRO com o motivo dizendo que o
     áudio foi salvo; caminhos do áudio gravados; partes NÃO apagadas (dá para reprocessar).
  C) dois robôs: o job já foi pego por outro. Esperado: este sai sem processar nada.
  D) Storage fora do ar o tempo todo (500): tenta 3 vezes e marca ERRO.

Uso: python tests/teste_worker.py   (precisa de ffmpeg/ffprobe no PATH)
"""
import importlib
import os
import shutil
import subprocess
import sys
import tempfile
import types

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
REUNIAO = "11111111-2222-3333-4444-555555555555"
SESSAO = "sess_teste"
BASE = f"reunioes/{REUNIAO}/{SESSAO}"


# ----------------------------------------------------------------------------- banco + Storage falsos
class Resp:
    def __init__(self, data):
        self.data = data


class Consulta:
    def __init__(self, db, tabela):
        self.db, self.tabela, self.filtros, self.op, self.valores, self._single = db, tabela, [], "select", None, False

    def select(self, *_a, **_k):
        self.op = "select"; return self

    def update(self, valores):
        self.op, self.valores = "update", valores; return self

    def eq(self, col, val):
        self.filtros.append((col, val)); return self

    def limit(self, _n):
        return self

    def single(self):
        self._single = True; return self

    def _linhas(self):
        return [l for l in self.db.tabelas[self.tabela] if all(l.get(c) == v for c, v in self.filtros)]

    def execute(self):
        linhas = self._linhas()
        if self.op == "update":
            for l in linhas:
                l.update(self.valores)
            self.db.historico.append((self.tabela, dict(self.valores), list(self.filtros)))
            return Resp([dict(l) for l in linhas])
        if self._single:
            return Resp(dict(linhas[0]) if linhas else None)
        return Resp([dict(l) for l in linhas])


class Bucket:
    def __init__(self, st):
        self.st = st

    def list(self, path):
        path = path.rstrip("/") + "/"
        nomes = sorted({k[len(path):].split("/")[0] for k in self.st.objetos if k.startswith(path)})
        return [{"name": n} for n in nomes]

    def download(self, path):
        return self.st.objetos[path]

    def remove(self, paths):
        for p in paths:
            self.st.objetos.pop(p, None)
            self.st.removidos.append(p)


class Storage:
    def __init__(self):
        self.objetos, self.removidos = {}, []

    def from_(self, _bucket):
        return Bucket(self)


class FakeSupabase:
    def __init__(self):
        self.tabelas = {"reuniao_processing_queue": [], "reunioes": []}
        self.historico = []
        self.storage = Storage()

    def table(self, nome):
        return Consulta(self, nome)


# ----------------------------------------------------------------------------- TUS falso
class ErroTus(Exception):
    def __init__(self, msg, status_code):
        super().__init__(msg); self.status_code = status_code


class FakeTus:
    """Regras do Storage simulado: arquivo > limite → 413; 'falhas_500' primeiras tentativas de vídeo → 500."""
    limite_bytes = 10 ** 12
    falhas_500_video = 0
    sempre_500 = False
    enviados = []          # (objeto, tamanho) na ordem
    st = None

    def __init__(self, url, headers):
        pass

    def uploader(self, file_path, chunk_size, metadata):
        cls = FakeTus
        obj = metadata["objectName"]

        class U:
            def upload(self_inner):
                tam = os.path.getsize(file_path)
                if cls.sempre_500:
                    raise ErroTus("Communication with tus server failed with status 500", 500)
                if tam > cls.limite_bytes:
                    raise ErroTus("Attempt to retrieve create file url with status 413", 413)
                if obj.endswith(".mp4") and cls.falhas_500_video > 0:
                    cls.falhas_500_video -= 1
                    raise ErroTus("Communication with tus server failed with status 500", 500)
                cls.enviados.append((obj, tam))
                cls.st.objetos[obj] = b"x"
        return U()


# ----------------------------------------------------------------------------- partes reais (ffmpeg)
def gerar_partes(pasta, n=3, segundos=12):
    caminhos = []
    for i in range(1, n + 1):
        out = os.path.join(pasta, f"part_{i:06d}.webm")
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", f"testsrc2=size=1280x720:rate=30",
            "-f", "lavfi", "-i", f"sine=frequency={300 + 100 * i}:sample_rate=48000",
            "-t", str(segundos), "-c:v", "libvpx", "-b:v", "3M", "-deadline", "realtime", "-cpu-used", "8",
            "-c:a", "libopus", "-y", out,
        ], check=True)
        caminhos.append(out)
    return caminhos


def preparar(fake, partes, job_status="PROCESSANDO"):
    fake.tabelas["reuniao_processing_queue"] = [{"id": "job-1", "reuniao_id": REUNIAO, "status": job_status, "log_text": ""}]
    fake.tabelas["reunioes"] = [{"id": REUNIAO, "gravacao_status": "PROCESSANDO", "gravacao_path": None,
                                 "gravacao_audio_path": None}]
    fake.storage.objetos = {f"{BASE}/{os.path.basename(p)}": open(p, "rb").read() for p in partes}
    fake.storage.removidos = []
    fake.historico = []
    FakeTus.enviados = []
    FakeTus.st = fake.storage


def carregar_worker(fake, max_upload):
    os.environ["SUPABASE_URL"] = "https://falso.supabase.co"
    os.environ["SUPABASE_KEY"] = "chave-falsa"
    os.environ["MAX_UPLOAD_BYTES"] = str(max_upload)
    os.environ["LOG_TO_DB"] = "0"
    fake_mod = types.ModuleType("supabase"); fake_mod.create_client = lambda *_a, **_k: fake
    sys.modules["supabase"] = fake_mod
    tus_pkg = types.ModuleType("tusclient"); tus_cli = types.ModuleType("tusclient.client")
    tus_cli.TusClient = FakeTus; tus_pkg.client = tus_cli
    sys.modules["tusclient"] = tus_pkg; sys.modules["tusclient.client"] = tus_cli
    sys.path.insert(0, RAIZ)
    sys.modules.pop("worker", None)
    w = importlib.import_module("worker")
    w.time.sleep = lambda *_a: None          # sem esperar nas novas tentativas
    return w


falhas = []


def confere(cond, msg):
    print(("  ✅ " if cond else "  ❌ ") + msg)
    if not cond:
        falhas.append(msg)


def main():
    if not shutil.which("ffmpeg"):
        sys.exit("precisa de ffmpeg no PATH")
    trabalho = tempfile.mkdtemp(prefix="teste_worker_")
    partes = gerar_partes(trabalho)
    print(f"partes geradas: {sum(os.path.getsize(p) for p in partes) / 1e6:.1f} MB em {len(partes)} arquivos")
    os.chdir(trabalho)

    # ---- A
    print("\nA) longa + Storage com 1 erro passageiro no vídeo")
    fake = FakeSupabase(); preparar(fake, partes)
    limite = 1_500_000
    w = carregar_worker(fake, limite)
    FakeTus.limite_bytes = int(limite * 1.05); FakeTus.falhas_500_video = 1; FakeTus.sempre_500 = False
    w.processar_fila()
    r = fake.tabelas["reunioes"][0]; q = fake.tabelas["reuniao_processing_queue"][0]
    ordem = [o for o, _ in FakeTus.enviados]
    confere(ordem and ordem[0].endswith("audio_completo.m4a"), f"áudio subiu primeiro (ordem: {[o.rsplit('/', 1)[-1] for o in ordem]})")
    confere(any(o.endswith("video_completo_render.mp4") for o in ordem), "vídeo subiu depois de 1 erro 500")
    tam_video = next((t for o, t in FakeTus.enviados if o.endswith(".mp4")), 0)
    confere(0 < tam_video <= limite, f"vídeo comprimido coube no limite ({tam_video / 1e6:.2f} MB ≤ {limite / 1e6:.2f} MB)")
    confere(r["gravacao_status"] == "CONCLUIDO" and not r.get("gravacao_erro"), f"reunião CONCLUIDO (está {r['gravacao_status']})")
    confere(r.get("gravacao_audio_path", "").endswith("audio_completo.m4a"), "caminho do áudio gravado na reunião")
    confere(q["status"] == "CONCLUIDO", f"job CONCLUIDO (está {q['status']})")
    confere(len(fake.storage.removidos) == len(partes), "partes apagadas só depois do sucesso")

    # ---- B
    print("\nB) vídeo não cabe de jeito nenhum")
    fake = FakeSupabase(); preparar(fake, partes)
    limite = 20_000
    w = carregar_worker(fake, limite)
    FakeTus.limite_bytes = 10 ** 9; FakeTus.falhas_500_video = 0
    try:
        w.processar_fila(); levantou = False
    except Exception:
        levantou = True
    r = fake.tabelas["reunioes"][0]; q = fake.tabelas["reuniao_processing_queue"][0]
    confere(levantou, "o robô termina com erro (aparece vermelho no GitHub)")
    confere(r["gravacao_status"] == "ERRO", f"reunião ERRO (está {r['gravacao_status']})")
    confere("Áudio salvo" in (r.get("gravacao_erro") or ""), f"motivo avisa que o áudio foi salvo: {(r.get('gravacao_erro') or '')[:90]!r}")
    confere((r.get("gravacao_audio_path") or "").endswith("audio_completo.m4a"), "áudio gravado na reunião mesmo com o vídeo falhando")
    confere(q["status"] == "ERRO", "job ERRO")
    confere(not fake.storage.removidos, "partes NÃO apagadas (dá para reprocessar)")

    # ---- C
    print("\nC) dois robôs: este chega depois")
    fake = FakeSupabase(); preparar(fake, partes)
    w = carregar_worker(fake, 10 ** 9)
    original = w.supabase.table

    def tabela_com_corrida(nome):
        c = original(nome)
        if nome == "reuniao_processing_queue":
            exec_orig = c.execute

            def execute():
                if c.op == "update" and ("status", "PROCESSANDO") in c.filtros:
                    fake.tabelas["reuniao_processing_queue"][0]["status"] = "PROCESSANDO_GITH"   # o outro pegou antes
                return exec_orig()
            c.execute = execute
        return c
    w.supabase.table = tabela_com_corrida
    w.processar_fila()
    r = fake.tabelas["reunioes"][0]
    confere(not FakeTus.enviados, "não enviou nada")
    confere(r["gravacao_status"] == "PROCESSANDO", "não mexeu na reunião (o outro robô cuida)")

    # ---- D
    print("\nD) Storage fora do ar (500 sempre)")
    fake = FakeSupabase(); preparar(fake, partes)
    w = carregar_worker(fake, 10 ** 9)
    FakeTus.sempre_500 = True
    tentativas = {"n": 0}
    up_orig = FakeTus.uploader

    def contar(self, **k):
        tentativas["n"] += 1
        return up_orig(self, **k)
    FakeTus.uploader = contar
    try:
        w.processar_fila()
    except Exception:
        pass
    FakeTus.uploader = up_orig; FakeTus.sempre_500 = False
    r = fake.tabelas["reunioes"][0]
    confere(tentativas["n"] == w.UPLOAD_TENTATIVAS, f"tentou {tentativas['n']} vezes (esperado {w.UPLOAD_TENTATIVAS})")
    confere(r["gravacao_status"] == "ERRO", "reunião ERRO com motivo")

    print("\n" + ("TUDO OK" if not falhas else f"{len(falhas)} FALHA(S): {falhas}"))
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
