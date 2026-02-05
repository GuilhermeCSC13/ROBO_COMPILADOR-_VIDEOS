import os
import subprocess
from flask import Flask, jsonify
from supabase import create_client
from tusclient import client

app = Flask(__name__)

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️  ERRO: Variáveis de ambiente faltando.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Buckets (ajuste se algum nome for diferente no seu projeto)
BUCKET_GRAVACOES = "gravacoes"

# Nomes padrão de arquivos gerados
VIDEO_FINAL_NAME = "video_completo_render.mp4"
AUDIO_FINAL_NAME = "audio_reuniao.m4a"  # leve e excelente pra voz (AAC)


def tus_upload(bucket: str, object_path: str, local_file: str, content_type: str):
    tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
    my_client = client.TusClient(
        url=tus_url,
        headers={"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"},
    )

    uploader = my_client.uploader(
        file_path=local_file,
        chunk_size=6 * 1024 * 1024,
        metadata={
            "bucketName": bucket,
            "objectName": object_path,
            "contentType": content_type,
            "cacheControl": "3600",
        },
    )
    uploader.upload()


def storage_list(bucket: str, path: str):
    return supabase.storage.from_(bucket).list(path) or []


def storage_download(bucket: str, object_path: str) -> bytes:
    return supabase.storage.from_(bucket).download(object_path)


def storage_remove(bucket: str, objects: list[str]):
    if not objects:
        return
    supabase.storage.from_(bucket).remove(objects)


def find_session_folder(reuniao_id: str) -> str | None:
    raiz = storage_list(BUCKET_GRAVACOES, f"reunioes/{reuniao_id}")
    sess = next((i["name"] for i in raiz if i.get("name", "").startswith("sess_")), None)
    return sess


def pick_existing_video_path(caminho_base: str) -> str | None:
    # procura mp4 final (padrão) ou qualquer mp4 que você já tenha gerado no passado
    arquivos = storage_list(BUCKET_GRAVACOES, caminho_base)
    # 1) preferencial: o padrão
    if any(a.get("name") == VIDEO_FINAL_NAME for a in arquivos):
        return f"{caminho_base}/{VIDEO_FINAL_NAME}"
    # 2) fallback: qualquer mp4 existente
    mp4s = [a.get("name") for a in arquivos if str(a.get("name", "")).lower().endswith(".mp4")]
    if mp4s:
        mp4s.sort()
        return f"{caminho_base}/{mp4s[-1]}"
    return None


def audio_exists(caminho_base: str) -> bool:
    arquivos = storage_list(BUCKET_GRAVACOES, caminho_base)
    return any(a.get("name") == AUDIO_FINAL_NAME for a in arquivos)


def parts_exist(caminho_base: str) -> list[dict]:
    arquivos = storage_list(BUCKET_GRAVACOES, caminho_base)
    partes = [p for p in arquivos if p.get("name", "").startswith("part_") and p.get("name", "").endswith(".webm")]
    partes.sort(key=lambda x: x["name"])
    return partes


def ffmpeg_concat_and_compress(list_file_path: str, output_mp4: str):
    ffmpeg_cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file_path,
        "-c:v", "libx264",
        "-crf", "28",
        "-preset", "veryfast",
        "-c:a", "aac",
        "-b:a", "64k",
        "-movflags", "+faststart",
        "-y",
        output_mp4
    ]
    subprocess.run(ffmpeg_cmd, check=True)


def ffmpeg_extract_audio(input_video: str, output_audio: str):
    # m4a (AAC) otimizado para voz
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", input_video,
        "-vn",
        "-c:a", "aac",
        "-b:a", "64k",
        "-movflags", "+faststart",
        "-y",
        output_audio
    ]
    subprocess.run(ffmpeg_cmd, check=True)


def get_size_mb(path: str) -> float:
    return os.path.getsize(path) / (1024 * 1024)


@app.route("/")
def home():
    return "🤖 Robô Pro (Compilador + Áudio) Ativo."


@app.route("/processar")
def processar():
    # 1) Pegar Job Pendente
    response = supabase.table("reuniao_processing_queue") \
        .select("*") \
        .eq("status", "PENDENTE") \
        .limit(1) \
        .execute()

    jobs = response.data
    if not jobs:
        return jsonify({"status": "Fila vazia"})

    job = jobs[0]
    reuniao_id = job["reuniao_id"]
    job_id = job["id"]

    print(f"🚀 Iniciando Job: {reuniao_id}")

    # Marca como processando
    supabase.table("reuniao_processing_queue") \
        .update({"status": "PROCESSANDO_RENDER", "log_text": "Verificando parts/vídeo/áudio..."}) \
        .eq("id", job_id).execute()

    local_files = []
    list_file_path = f"/tmp/{reuniao_id}_list.txt"
    local_video_mp4 = f"/tmp/{reuniao_id}_video.mp4"
    local_audio_m4a = f"/tmp/{reuniao_id}_audio.m4a"

    try:
        # 2) Ler reunião no banco (pra saber se já tem áudio gravado em coluna)
        reuniao = supabase.table("reunioes").select("*").eq("id", reuniao_id).single().execute().data or {}

        # 3) Descobrir pasta sess_
        sessao_folder = find_session_folder(reuniao_id)
        if not sessao_folder:
            raise Exception("Pasta sessão não encontrada (sess_...).")

        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"

        # 4) Checar existência
        partes = parts_exist(caminho_base)

        # vídeo existente no storage
        storage_video_path = reuniao.get("gravacao_path") or pick_existing_video_path(caminho_base)
        # áudio existe no storage OU já está apontado na tabela
        has_audio_in_db = bool(reuniao.get("gravacao_audio_path"))
        has_audio_in_bucket = audio_exists(caminho_base)
        has_audio = has_audio_in_db or has_audio_in_bucket

        # 5) CASO A: tem parts -> compila vídeo + extrai áudio + apaga parts
        if partes:
            supabase.table("reuniao_processing_queue") \
                .update({"log_text": f"Encontradas {len(partes)} parts. Compilando MP4 + extraindo áudio..."}) \
                .eq("id", job_id).execute()

            # Download parts e gerar list.txt do concat
            with open(list_file_path, "w") as f_list:
                for p in partes:
                    name = p["name"]
                    local_part = f"/tmp/{name}"
                    data = storage_download(BUCKET_GRAVACOES, f"{caminho_base}/{name}")
                    with open(local_part, "wb") as fp:
                        fp.write(data)
                    local_files.append(local_part)
                    f_list.write(f"file '{local_part}'\n")

            # Compilar mp4
            ffmpeg_concat_and_compress(list_file_path, local_video_mp4)
            video_mb = get_size_mb(local_video_mp4)
            print(f"✅ Vídeo final: {video_mb:.2f} MB")

            # Extrair áudio
            ffmpeg_extract_audio(local_video_mp4, local_audio_m4a)
            audio_mb = get_size_mb(local_audio_m4a)
            print(f"✅ Áudio final: {audio_mb:.2f} MB")

            # Upload vídeo + áudio
            video_dest = f"{caminho_base}/{VIDEO_FINAL_NAME}"
            audio_dest = f"{caminho_base}/{AUDIO_FINAL_NAME}"

            tus_upload(BUCKET_GRAVACOES, video_dest, local_video_mp4, "video/mp4")
            tus_upload(BUCKET_GRAVACOES, audio_dest, local_audio_m4a, "audio/mp4")  # m4a

            # Apagar parts (depois do upload ok)
            caminhos_para_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
            for i in range(0, len(caminhos_para_apagar), 20):
                storage_remove(BUCKET_GRAVACOES, caminhos_para_apagar[i:i+20])

            # Atualiza reunião (vídeo + áudio)
            supabase.table("reunioes").update({
                "gravacao_bucket": BUCKET_GRAVACOES,
                "gravacao_path": video_dest,
                "gravacao_status": "CONCLUIDO",
                "gravacao_mime": "video/mp4",
                "gravacao_size_bytes": os.path.getsize(local_video_mp4),

                "gravacao_audio_bucket": BUCKET_GRAVACOES,
                "gravacao_audio_path": audio_dest,
                "gravacao_audio_mime": "audio/mp4",
                "gravacao_audio_size_bytes": os.path.getsize(local_audio_m4a),
            }).eq("id", reuniao_id).execute()

            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": f"Sucesso. Vídeo {video_mb:.1f}MB / Áudio {audio_mb:.1f}MB. Parts apagadas."
            }).eq("id", job_id).execute()

            # limpeza local
            if os.path.exists(list_file_path): os.remove(list_file_path)
            if os.path.exists(local_video_mp4): os.remove(local_video_mp4)
            if os.path.exists(local_audio_m4a): os.remove(local_audio_m4a)
            for f in local_files:
                if os.path.exists(f): os.remove(f)

            return jsonify({
                "status": "Sucesso",
                "id": reuniao_id,
                "acao": "COMPILOU_VIDEO_E_AUDIO_E_APAGOU_PARTS",
                "video_mb": video_mb,
                "audio_mb": audio_mb,
            })

        # 6) CASO B: não tem parts. Se tem vídeo mp4 e não tem áudio -> extrai só áudio
        if storage_video_path and not has_audio:
            supabase.table("reuniao_processing_queue") \
                .update({"log_text": "Sem parts. Vídeo existe e áudio ausente. Extraindo áudio..."}) \
                .eq("id", job_id).execute()

            # baixar mp4
            video_bytes = storage_download(BUCKET_GRAVACOES, storage_video_path)
            with open(local_video_mp4, "wb") as f:
                f.write(video_bytes)

            # extrair áudio
            ffmpeg_extract_audio(local_video_mp4, local_audio_m4a)
            audio_mb = get_size_mb(local_audio_m4a)

            audio_dest = f"{caminho_base}/{AUDIO_FINAL_NAME}"
            tus_upload(BUCKET_GRAVACOES, audio_dest, local_audio_m4a, "audio/mp4")  # m4a

            # atualizar reunião (somente áudio; mantém vídeo como está)
            supabase.table("reunioes").update({
                "gravacao_audio_bucket": BUCKET_GRAVACOES,
                "gravacao_audio_path": audio_dest,
                "gravacao_audio_mime": "audio/mp4",
                "gravacao_audio_size_bytes": os.path.getsize(local_audio_m4a),
            }).eq("id", reuniao_id).execute()

            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": f"Áudio extraído do MP4. Áudio {audio_mb:.1f}MB."
            }).eq("id", job_id).execute()

            # limpar local
            if os.path.exists(local_video_mp4): os.remove(local_video_mp4)
            if os.path.exists(local_audio_m4a): os.remove(local_audio_m4a)

            return jsonify({
                "status": "Sucesso",
                "id": reuniao_id,
                "acao": "EXTRAIU_AUDIO_APENAS",
                "audio_mb": audio_mb,
                "video_path": storage_video_path,
            })

        # 7) CASO C: já tem vídeo + áudio -> não faz nada
        if storage_video_path and has_audio:
            supabase.table("reuniao_processing_queue").update({
                "status": "CONCLUIDO",
                "log_text": "Já existe vídeo e áudio. Nenhuma ação necessária."
            }).eq("id", job_id).execute()

            return jsonify({
                "status": "OK",
                "id": reuniao_id,
                "acao": "NADA_A_FAZER",
                "video_path": storage_video_path,
                "audio_in_db": bool(reuniao.get("gravacao_audio_path")),
                "audio_in_bucket": has_audio_in_bucket,
            })

        # 8) Sem parts, sem vídeo -> erro
        raise Exception("Sem parts e sem vídeo mp4 encontrado para esta reunião.")

    except Exception as e:
        msg = str(e)
        print(f"❌ Erro: {msg}")
        supabase.table("reuniao_processing_queue").update({"status": "ERRO", "log_text": msg}).eq("id", job_id).execute()

        # tentar limpar lixo local
        for p in [list_file_path, local_video_mp4, local_audio_m4a]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass
        for f in local_files:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass

        return jsonify({"status": "Erro", "msg": msg}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
