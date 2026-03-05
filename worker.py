import os
import subprocess
import time
from supabase import create_client
from tusclient import client as tus_client

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("⚠️ Variáveis de ambiente SUPABASE_URL e SUPABASE_KEY são obrigatórias.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET = "gravacoes"

# =============================================================================
# LOG PASSO A PASSO (console + opcional no banco)
# =============================================================================
LOG_TO_DB = os.getenv("LOG_TO_DB", "1").lower() not in ("0", "false", "no")
LOG_MAX_CHARS = 8000


def _db_append_log(job_id: str, line: str):
    if not (LOG_TO_DB and job_id):
        return
    try:
        cur = (
            supabase.table("reuniao_processing_queue")
            .select("log_text")
            .eq("id", job_id)
            .single()
            .execute()
        ).data or {}
        prev = cur.get("log_text") or ""
        new_text = (prev + "\n" + line).strip()
        if len(new_text) > LOG_MAX_CHARS:
            new_text = new_text[-LOG_MAX_CHARS:]
        supabase.table("reuniao_processing_queue").update({"log_text": new_text}).eq("id", job_id).execute()
    except Exception:
        pass


def log(msg: str, job_id: str = None, icon: str = "ℹ️", db: bool = False):
    line = f"{icon} {msg}"
    print(line, flush=True)
    if db:
        _db_append_log(job_id, line)


def timed(label: str, job_id: str = None, db: bool = False):
    t0 = time.perf_counter()

    def end(extra: str = ""):
        dt = time.perf_counter() - t0
        s = f"{label} levou {dt:.1f}s"
        if extra:
            s += f" | {extra}"
        log(s, job_id, icon="⏱️", db=db)

    return end


# =============================================================================
# UPLOAD (TUS / RESUMABLE)
# =============================================================================
def tus_upload(local_path: str, object_name: str, content_type: str, job_id: str = None):
    log(f"TUS upload -> {object_name} ({content_type})", job_id, icon="⬆️", db=True)

    tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
    headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"}
    my_client = tus_client.TusClient(url=tus_url, headers=headers)

    uploader = my_client.uploader(
        file_path=local_path,
        chunk_size=6 * 1024 * 1024,
        metadata={
            "bucketName": BUCKET,
            "objectName": object_name,
            "contentType": content_type,
            "cacheControl": "3600",
        },
    )
    uploader.upload()

    log(f"Upload concluído -> {object_name}", job_id, icon="✅", db=True)


# =============================================================================
# HELPERS
# =============================================================================
def safe_rm(p):
    try:
        if p and os.path.exists(p):
            os.remove(p)
    except Exception:
        pass


def list_storage(path: str):
    return supabase.storage.from_(BUCKET).list(path) or []


def download_storage(path: str) -> bytes:
    return supabase.storage.from_(BUCKET).download(path)


def remove_storage(paths: list):
    if paths:
        supabase.storage.from_(BUCKET).remove(paths)


def find_sessao_folder(reuniao_id: str):
    raiz = list_storage(f"reunioes/{reuniao_id}")
    return next((i["name"] for i in raiz if i.get("name", "").startswith("sess_")), None)


def storage_file_exists(object_path: str) -> bool:
    if not object_path or "/" not in object_path:
        return False
    dir_path, fname = object_path.rsplit("/", 1)
    try:
        items = list_storage(dir_path)
        return any(i.get("name") == fname for i in items)
    except Exception:
        return False


# =============================================================================
# FFMPEG
# =============================================================================
def run_ffmpeg(cmd: list, title: str, job_id: str = None):
    log(title, job_id, icon="🎬", db=True)
    # Mostra comando pra debug
    log("CMD: " + " ".join(cmd), job_id, icon="🧾", db=False)
    subprocess.run(cmd, check=True)
    log(f"OK: {title}", job_id, icon="✅", db=True)


# ---- ÁUDIO BLINDADO (WAV por part -> concat WAV) ----
def ffmpeg_extract_audio_part_to_wav(input_webm: str, out_wav: str, job_id: str = None):
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-fflags", "+genpts",
        "-i", input_webm,
        "-vn",
        "-ac", "2",
        "-ar", "48000",
        "-af", "asetpts=PTS-STARTPTS,aresample=async=1:first_pts=0",
        "-c:a", "pcm_s16le",
        "-y", out_wav
    ]
    run_ffmpeg(cmd, f"Extrair áudio da part -> WAV: {out_wav}", job_id)


def ffmpeg_concat_wavs_copy(list_wav_path: str, out_wav: str, job_id: str = None):
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-f", "concat", "-safe", "0",
        "-i", list_wav_path,
        "-c", "copy",
        "-y", out_wav
    ]
    run_ffmpeg(cmd, f"Concat WAVs (copy) -> {out_wav}", job_id)


def ffmpeg_wav_to_m4a(in_wav: str, out_m4a: str, job_id: str = None):
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-i", in_wav,
        "-c:a", "aac",
        "-b:a", "64k",
        "-ar", "44100",
        "-af", "aresample=async=1:first_pts=0",
        "-y", out_m4a
    ]
    run_ffmpeg(cmd, f"WAV -> M4A (AAC) -> {out_m4a}", job_id)


# ---- VÍDEO COMPLETO (remux webm->mkv por part -> concat mkv copy) ----
def ffmpeg_remux_video_webm_to_mkv(input_webm: str, out_mkv: str, job_id: str = None):
    """
    Remuxa o container (SEM reencode) para MKV, estabiliza concat do vídeo.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-fflags", "+genpts",
        "-i", input_webm,
        "-map", "0:v:0",   # só vídeo
        "-c", "copy",
        "-y", out_mkv
    ]
    run_ffmpeg(cmd, f"Remux vídeo WEBM->MKV (copy) -> {out_mkv}", job_id)


def ffmpeg_concat_mkvs_copy(list_mkv_path: str, out_mkv: str, job_id: str = None):
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-f", "concat", "-safe", "0",
        "-i", list_mkv_path,
        "-c", "copy",
        "-y", out_mkv
    ]
    run_ffmpeg(cmd, f"Concat MKVs (copy) -> {out_mkv}", job_id)


def ffmpeg_make_mp4_from_video_mkv_and_external_audio(video_mkv: str, in_wav_audio: str, out_mp4: str, job_id: str = None):
    """
    MP4 final:
      - VÍDEO: vem do MKV concatenado (completo)
      - ÁUDIO: vem do WAV contínuo (blindado)
    """
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-fflags", "+genpts",
        "-i", video_mkv,
        "-i", in_wav_audio,

        "-map", "0:v:0",
        "-map", "1:a:0",

        # vídeo simples (rápido)
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "34",
        "-pix_fmt", "yuv420p",
        "-r", "15",

        # áudio
        "-c:a", "aac",
        "-b:a", "96k",
        "-ar", "48000",

        "-shortest",
        "-movflags", "+faststart",
        "-y", out_mp4
    ]
    run_ffmpeg(cmd, f"MP4 final (vídeo completo + áudio WAV) -> {out_mp4}", job_id)


# ---- CASO 2: extrair áudio do mp4 existente (mantido) ----
def ffmpeg_extract_audio_m4a(input_video: str, output_audio: str, job_id: str = None):
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", input_video,
        "-vn",
        "-c:a", "aac",
        "-b:a", "64k",
        "-ar", "44100",
        "-af", "aresample=async=1:first_pts=0",
        "-y", output_audio
    ]
    run_ffmpeg(cmd, f"Extrair Áudio (M4A) -> {output_audio}", job_id)


# =============================================================================
# WORKER
# =============================================================================
def processar_fila():
    log("Robô GitHub Worker Iniciado (Vídeo + Áudio)...", icon="🤖")

    log("Buscando 1 job com status=PROCESSANDO...", icon="➡️")
    response = (
        supabase.table("reuniao_processing_queue")
        .select("*")
        .eq("status", "PROCESSANDO")
        .limit(1)
        .execute()
    )

    jobs = response.data
    if not jobs:
        log("Fila vazia.", icon="✅")
        return

    job = jobs[0]
    reuniao_id = job["reuniao_id"]
    job_id = job["id"]
    log(f"Job encontrado: {job_id} | reunião: {reuniao_id}", job_id, icon="✅", db=True)

    log("Travando job (PROCESSANDO -> PROCESSANDO_GITH)...", job_id, icon="➡️", db=True)
    supabase.table("reuniao_processing_queue").update({
        "status": "PROCESSANDO_GITH",
        "log_text": "GitHub Actions: Iniciando processamento..."
    }).eq("id", job_id).execute()
    log("Job travado.", job_id, icon="✅", db=True)

    local_files = []
    output_video = f"output_{reuniao_id}.mp4"
    output_audio = f"audio_{reuniao_id}.m4a"

    # arquivos do fluxo
    list_parts_path = f"list_parts_{reuniao_id}.txt"
    list_wav_parts_path = f"list_wav_parts_{reuniao_id}.txt"
    list_mkv_parts_path = f"list_mkv_parts_{reuniao_id}.txt"

    wav_audio = f"audio_{reuniao_id}.wav"
    concat_video_mkv = f"video_concat_{reuniao_id}.mkv"

    try:
        log("Carregando registro da reunião (reunioes)...", job_id, icon="➡️", db=True)
        reuniao_resp = supabase.table("reunioes").select("*").eq("id", reuniao_id).single().execute()
        reuniao = reuniao_resp.data or {}
        log("Registro carregado.", job_id, icon="✅", db=True)

        gravacao_path = reuniao.get("gravacao_path")
        gravacao_bucket = reuniao.get("gravacao_bucket") or BUCKET

        gravacao_audio_path = reuniao.get("gravacao_audio_path")
        gravacao_audio_bucket = reuniao.get("gravacao_audio_bucket") or gravacao_bucket

        log(f"gravacao_path={gravacao_path} | gravacao_audio_path={gravacao_audio_path}", job_id, icon="ℹ️", db=True)

        log("Procurando pasta de sessão (sess_*)...", job_id, icon="➡️", db=True)
        sessao_folder = find_sessao_folder(reuniao_id)

        caminho_base = None
        partes = []

        if sessao_folder:
            caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
            log(f"Sessão encontrada: {sessao_folder}", job_id, icon="✅", db=True)

            log(f"Listando arquivos em: {caminho_base}", job_id, icon="➡️", db=True)
            arquivos = list_storage(caminho_base)

            partes = [
                p for p in arquivos
                if p.get("name", "").startswith("part_") and p.get("name", "").endswith(".webm")
            ]
            partes.sort(key=lambda x: x["name"])
            log(f"Parts encontradas: {len(partes)}", job_id, icon="ℹ️", db=True)
        else:
            log("Nenhuma pasta sess_* encontrada.", job_id, icon="⚠️", db=True)

        has_parts = len(partes) > 0

        log("Verificando se já existe vídeo/áudio final no Storage...", job_id, icon="➡️", db=True)
        video_exists = storage_file_exists(gravacao_path) if gravacao_path else False
        audio_exists = storage_file_exists(gravacao_audio_path) if gravacao_audio_path else False
        log(f"video_exists={video_exists} | audio_exists={audio_exists}", job_id, icon="ℹ️", db=True)

        # =========================
        # CASO 1: TEM PARTS
        # =========================
        if has_parts:
            log("Modo PARTS: áudio blindado + vídeo completo (MKV concat).", job_id, icon="✅", db=True)

            # 1) Baixar parts e criar list_parts
            log(f"Baixando {len(partes)} parts...", job_id, icon="⬇️", db=True)
            tdl = timed("Download das parts", job_id, db=True)

            with open(list_parts_path, "w") as f_list:
                for idx, p in enumerate(partes, start=1):
                    name = p["name"]
                    full_path = f"{caminho_base}/{name}"
                    local_webm = name

                    t1 = timed(f"[{idx}/{len(partes)}] Download {name}", job_id, db=True)
                    with open(local_webm, "wb") as f:
                        f.write(download_storage(full_path))
                    local_files.append(local_webm)
                    f_list.write(f"file '{local_webm}'\n")
                    t1(f"{os.path.getsize(local_webm)} bytes")

            tdl("OK")
            local_files.append(list_parts_path)

            # 2) ÁUDIO: WAV por part + concat WAV
            log("Áudio: gerando WAV por part...", job_id, icon="➡️", db=True)
            with open(list_wav_parts_path, "w") as f_list:
                for idx, p in enumerate(partes, start=1):
                    local_webm = p["name"]
                    part_wav = f"apart_{idx:05d}.wav"

                    tW = timed(f"[{idx}/{len(partes)}] WAV {local_webm}", job_id, db=True)
                    ffmpeg_extract_audio_part_to_wav(local_webm, part_wav, job_id)
                    tW(f"{os.path.getsize(part_wav)} bytes")

                    local_files.append(part_wav)
                    f_list.write(f"file '{part_wav}'\n")

            local_files.append(list_wav_parts_path)

            tCW = timed("Concat WAVs", job_id, db=True)
            ffmpeg_concat_wavs_copy(list_wav_parts_path, wav_audio, job_id)
            tCW(f"{os.path.getsize(wav_audio)} bytes")
            local_files.append(wav_audio)

            tm4a = timed("Gerar M4A final", job_id, db=True)
            ffmpeg_wav_to_m4a(wav_audio, output_audio, job_id)
            tm4a(f"{os.path.getsize(output_audio)} bytes")
            local_files.append(output_audio)

            # 3) VÍDEO: remux vídeo por part -> MKV + concat MKV copy
            log("Vídeo: remux WEBM->MKV por part (copy) e concat MKV...", job_id, icon="➡️", db=True)
            with open(list_mkv_parts_path, "w") as f_list:
                for idx, p in enumerate(partes, start=1):
                    local_webm = p["name"]
                    part_mkv = f"vpart_{idx:05d}.mkv"

                    tV = timed(f"[{idx}/{len(partes)}] Remux vídeo {local_webm}", job_id, db=True)
                    ffmpeg_remux_video_webm_to_mkv(local_webm, part_mkv, job_id)
                    tV(f"{os.path.getsize(part_mkv)} bytes")

                    local_files.append(part_mkv)
                    f_list.write(f"file '{part_mkv}'\n")

            local_files.append(list_mkv_parts_path)

            tCV = timed("Concat MKVs (vídeo)", job_id, db=True)
            ffmpeg_concat_mkvs_copy(list_mkv_parts_path, concat_video_mkv, job_id)
            tCV(f"{os.path.getsize(concat_video_mkv)} bytes")
            local_files.append(concat_video_mkv)

            # 4) MP4 final: vídeo completo + áudio WAV
            tmp4 = timed("Gerar MP4 final", job_id, db=True)
            ffmpeg_make_mp4_from_video_mkv_and_external_audio(concat_video_mkv, wav_audio, output_video, job_id)
            tmp4(f"{os.path.getsize(output_video)} bytes")
            local_files.append(output_video)

            # uploads
            path_video = f"{caminho_base}/video_completo_render.mp4"
            path_audio = f"{caminho_base}/audio_completo.m4a"

            tus_upload(output_video, path_video, "video/mp4", job_id)
            tus_upload(output_audio, path_audio, "audio/mp4", job_id)

            # atualiza banco
            log("Atualizando tabela reunioes com paths finais...", job_id, icon="➡️", db=True)
            supabase.table("reunioes").update({
                "gravacao_bucket": BUCKET,
                "gravacao_path": path_video,
                "gravacao_status": "CONCLUIDO",
                "gravacao_mime": "video/mp4",
                "gravacao_size_bytes": os.path.getsize(output_video),

                "gravacao_audio_bucket": BUCKET,
                "gravacao_audio_path": path_audio,
                "gravacao_audio_mime": "audio/mp4",
                "gravacao_audio_size_bytes": os.path.getsize(output_audio),
            }).eq("id", reuniao_id).execute()
            log("Reunião atualizada.", job_id, icon="✅", db=True)

            # apagar parts no storage
            log("Apagando parts do storage...", job_id, icon="🧹", db=True)
            caminhos_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
            for i in range(0, len(caminhos_apagar), 20):
                remove_storage(caminhos_apagar[i:i + 20])
            log("Parts removidas.", job_id, icon="✅", db=True)

            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Sucesso: MP4 completo (MKV concat vídeo) + áudio blindado (WAV por part -> concat -> M4A)."
            }).eq("id", job_id).execute()

            log("Concluído (vídeo completo + áudio completo).", job_id, icon="🎉", db=True)
            return

        # =========================
        # CASO 2: NÃO TEM PARTS
        # =========================
        if video_exists and audio_exists:
            log("Já existe vídeo e áudio. Nada a fazer.", job_id, icon="✅", db=True)
            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Sem ação: vídeo e áudio já existentes."
            }).eq("id", job_id).execute()
            return

        if video_exists and (not audio_exists):
            log("Vídeo existe e áudio não existe. Extraindo apenas o áudio...", job_id, icon="🎧", db=True)

            local_mp4 = f"orig_{reuniao_id}.mp4"
            tdl2 = timed(f"Download MP4 existente -> {local_mp4}", job_id, db=True)
            with open(local_mp4, "wb") as f:
                f.write(download_storage(gravacao_path))
            local_files.append(local_mp4)
            tdl2(f"{os.path.getsize(local_mp4)} bytes")

            tEx = timed("Extrair áudio do MP4", job_id, db=True)
            ffmpeg_extract_audio_m4a(local_mp4, output_audio, job_id)
            tEx(f"{os.path.getsize(output_audio)} bytes")
            local_files.append(output_audio)

            if caminho_base:
                path_audio = f"{caminho_base}/audio_completo.m4a"
            else:
                dir_path = gravacao_path.rsplit("/", 1)[0]
                path_audio = f"{dir_path}/audio_completo.m4a"

            tus_upload(output_audio, path_audio, "audio/mp4", job_id)

            supabase.table("reunioes").update({
                "gravacao_audio_bucket": BUCKET,
                "gravacao_audio_path": path_audio,
                "gravacao_audio_mime": "audio/mp4",
                "gravacao_audio_size_bytes": os.path.getsize(output_audio),
            }).eq("id", reuniao_id).execute()

            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Sucesso: Áudio extraído do MP4 existente."
            }).eq("id", job_id).execute()

            log("Concluído (mp4->audio).", job_id, icon="🎉", db=True)
            return

        msg = "Nenhuma part .webm encontrada e também não foi encontrado vídeo MP4 para extrair áudio."
        log(msg, job_id, icon="❌", db=True)
        supabase.table("reuniao_processing_queue").update({
            "status": "ERRO",
            "log_text": msg
        }).eq("id", job_id).execute()
        raise Exception(msg)

    except Exception as e:
        log(f"ERRO: {e}", job_id, icon="❌", db=True)
        supabase.table("reuniao_processing_queue").update({
            "status": "ERRO",
            "log_text": str(e)
        }).eq("id", job_id).execute()
        raise
    finally:
        log("Limpando arquivos locais temporários...", job_id, icon="🧹", db=False)
        for f in local_files + [
            list_parts_path, list_wav_parts_path, list_mkv_parts_path,
            wav_audio, concat_video_mkv, output_video, output_audio
        ]:
            safe_rm(f)
        log("Limpeza concluída.", job_id, icon="✅", db=False)


if __name__ == "__main__":
    processar_fila()
