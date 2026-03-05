import os
import subprocess
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
# UPLOAD (TUS / RESUMABLE)
# =============================================================================
def tus_upload(local_path: str, object_name: str, content_type: str):
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
    # checa no diretório pelo nome do arquivo
    if not object_path or "/" not in object_path:
        return False
    dir_path, fname = object_path.rsplit("/", 1)
    try:
        items = list_storage(dir_path)
        return any(i.get("name") == fname for i in items)
    except Exception:
        return False

# =============================================================================
# FFMPEG (BLINDADO PARA MEDIARECORDER WEBM CHUNKED)
# =============================================================================
def ffmpeg_normalize_part_to_mp4(input_webm: str, output_mp4: str):
    """
    Normaliza cada chunk WebM para um MP4 estável (timestamps e áudio contínuo).
    - Corrige PTS/DTS (genpts)
    - Força áudio contínuo (aresample async + first_pts=0)
    - Força vídeo CFR (vsync cfr)
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-fflags", "+genpts",
        "-i", input_webm,

        # Vídeo
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "28",
        "-pix_fmt", "yuv420p",
        "-vsync", "cfr",

        # Áudio
        "-c:a", "aac",
        "-b:a", "96k",
        "-ar", "48000",
        "-ac", "2",
        "-af", "aresample=async=1:first_pts=0",

        "-movflags", "+faststart",
        "-y", output_mp4
    ]
    subprocess.run(cmd, check=True)

def ffmpeg_concat_mp4_copy(list_file_path: str, output_mp4: str):
    """
    Concatena MP4 normalizados sem reencode (mais seguro).
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file_path,
        "-c", "copy",
        "-movflags", "+faststart",
        "-y", output_mp4
    ]
    subprocess.run(cmd, check=True)

def ffmpeg_extract_audio_m4a(input_video: str, output_audio: str):
    """
    Extrai áudio do MP4 final, garantindo timeline contínua.
    """
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
    subprocess.run(cmd, check=True)

# =============================================================================
# WORKER
# =============================================================================
def processar_fila():
    print("🤖 Robô GitHub Worker Iniciado (Vídeo + Áudio)...")

    # 1) Pegar 1 job pendente (ajuste conforme seu status real)
    response = (
        supabase.table("reuniao_processing_queue")
        .select("*")
        .eq("status", "PROCESSANDO")  # <-- mantém como você está usando
        .limit(1)
        .execute()
    )

    jobs = response.data
    if not jobs:
        print("zzZ Fila vazia.")
        return

    job = jobs[0]
    reuniao_id = job["reuniao_id"]
    job_id = job["id"]
    print(f"🚀 Processando Reunião: {reuniao_id}")

    # trava o job
    supabase.table("reuniao_processing_queue").update({
        "status": "PROCESSANDO_GITH",
        "log_text": "GitHub Actions: Verificando mídia (parts/mp4/áudio)..."
    }).eq("id", job_id).execute()

    local_files = []
    list_file_path = f"list_{reuniao_id}.txt"            # (pode ficar; não é mais usado no fluxo de parts)
    list_norm_path = f"list_norm_{reuniao_id}.txt"       # ✅ novo
    output_video = f"output_{reuniao_id}.mp4"
    output_audio = f"audio_{reuniao_id}.m4a"

    try:
        # 2) Pega registro da reunião para saber paths existentes
        reuniao_resp = supabase.table("reunioes").select("*").eq("id", reuniao_id).single().execute()
        reuniao = reuniao_resp.data or {}

        gravacao_path = reuniao.get("gravacao_path")  # vídeo (mp4)
        gravacao_bucket = reuniao.get("gravacao_bucket") or BUCKET

        gravacao_audio_path = reuniao.get("gravacao_audio_path")  # áudio (m4a)
        gravacao_audio_bucket = reuniao.get("gravacao_audio_bucket") or gravacao_bucket

        # 3) Descobre sessão e parts
        print("📂 Listando arquivos no Supabase...")
        sessao_folder = find_sessao_folder(reuniao_id)

        caminho_base = None
        partes = []

        if sessao_folder:
            caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
            arquivos = list_storage(caminho_base)
            partes = [p for p in arquivos if p.get("name", "").startswith("part_") and p.get("name", "").endswith(".webm")]
            partes.sort(key=lambda x: x["name"])

        has_parts = len(partes) > 0

        # 4) Detecta vídeo e áudio existentes (conforme paths do banco)
        video_exists = storage_file_exists(gravacao_path) if gravacao_path else False
        audio_exists = storage_file_exists(gravacao_audio_path) if gravacao_audio_path else False

        # =========================
        # CASO 1: TEM PARTS -> NORMALIZA CADA PART -> CONCAT COPY -> EXTRAI ÁUDIO -> APAGA PARTS
        # =========================
        if has_parts:
            print(f"⬇️ Baixando {len(partes)} partes...")

            normalized_files = []

            for idx, p in enumerate(partes, start=1):
                name = p["name"]
                local_webm = name  # ok no GitHub runner
                full_path = f"{caminho_base}/{name}"

                print(f"   - Baixando {name}...")
                with open(local_webm, "wb") as f_video:
                    data = download_storage(full_path)
                    f_video.write(data)

                local_files.append(local_webm)

                # ✅ normaliza cada part para MP4 estável (tira o “relógio torto” do WebM)
                local_norm = f"norm_{idx:05d}.mp4"
                print(f"   - Normalizando {name} -> {local_norm} ...")
                ffmpeg_normalize_part_to_mp4(local_webm, local_norm)

                local_files.append(local_norm)
                normalized_files.append(local_norm)

            if len(normalized_files) < 1:
                raise Exception("Nenhuma part normalizada gerada. Verifique download/ffmpeg.")

            # lista para concat dos MP4 normalizados
            with open(list_norm_path, "w") as f_list:
                for nf in normalized_files:
                    f_list.write(f"file '{nf}'\n")

            print("🎬 Concatenando MP4 normalizados (copy, sem reencode)...")
            ffmpeg_concat_mp4_copy(list_norm_path, output_video)

            print("🎵 Extraindo Áudio (M4A) para a IA...")
            ffmpeg_extract_audio_m4a(output_video, output_audio)

            # uploads
            path_video = f"{caminho_base}/video_completo_render.mp4"
            path_audio = f"{caminho_base}/audio_completo.m4a"

            print("⬆️ Uploading Vídeo...")
            tus_upload(output_video, path_video, "video/mp4")

            print("⬆️ Uploading Áudio...")
            tus_upload(output_audio, path_audio, "audio/mp4")

            # atualiza banco
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

            # apaga parts no storage
            print("🧹 Apagando parts do storage...")
            caminhos_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
            for i in range(0, len(caminhos_apagar), 20):
                remove_storage(caminhos_apagar[i:i+20])

            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Sucesso: Vídeo e Áudio gerados (parts normalizadas + concat copy; parts removidas)."
            }).eq("id", job_id).execute()

            print("✅ Concluído (parts->norm mp4->concat mp4->audio).")
            return

        # =========================
        # CASO 2: NÃO TEM PARTS
        # - se tem vídeo e não tem áudio -> extrai só áudio
        # - se tem vídeo e tem áudio -> noop
        # - se não tem vídeo -> erro
        # =========================

        if video_exists and audio_exists:
            print("✅ Já existe vídeo e áudio. Nada a fazer.")
            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Sem ação: vídeo e áudio já existentes."
            }).eq("id", job_id).execute()
            return

        if video_exists and (not audio_exists):
            print("🎧 Vídeo existe e áudio não existe. Extraindo apenas o áudio...")

            # baixa o vídeo existente
            local_mp4 = f"orig_{reuniao_id}.mp4"
            with open(local_mp4, "wb") as f:
                f.write(download_storage(gravacao_path))
            local_files.append(local_mp4)

            ffmpeg_extract_audio_m4a(local_mp4, output_audio)

            # define destino do áudio
            if caminho_base:
                path_audio = f"{caminho_base}/audio_completo.m4a"
            else:
                # mesmo diretório do vídeo
                dir_path = gravacao_path.rsplit("/", 1)[0]
                path_audio = f"{dir_path}/audio_completo.m4a"

            print("⬆️ Uploading Áudio...")
            tus_upload(output_audio, path_audio, "audio/mp4")

            # atualiza banco
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

            print("✅ Concluído (mp4->audio).")
            return

        # não tem parts e também não tem mp4 (ou path inválido)
        msg = "Nenhuma part .webm encontrada e também não foi encontrado vídeo MP4 para extrair áudio."
        print(f"❌ ERRO: {msg}")
        supabase.table("reuniao_processing_queue").update({
            "status": "ERRO",
            "log_text": msg
        }).eq("id", job_id).execute()
        raise Exception(msg)

    except Exception as e:
        print(f"❌ ERRO: {e}")
        supabase.table("reuniao_processing_queue").update({
            "status": "ERRO",
            "log_text": str(e)
        }).eq("id", job_id).execute()
        raise
    finally:
        # limpeza local
        for f in local_files + [list_file_path, list_norm_path, output_video, output_audio]:
            safe_rm(f)

if __name__ == "__main__":
    processar_fila()
