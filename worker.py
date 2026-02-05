import os
import subprocess
from supabase import create_client
from tusclient import client

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("⚠️ Variáveis de ambiente SUPABASE_URL e SUPABASE_KEY são obrigatórias.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ajustes do fluxo “antigo” + extração de áudio:
BATCH_DELETE_SIZE = 100
TUS_CHUNK_SIZE = 50 * 1024 * 1024  # 50MB


def tus_upload(my_client, file_path, bucket, object_name, content_type):
    uploader = my_client.uploader(
        file_path=file_path,
        chunk_size=TUS_CHUNK_SIZE,
        metadata={
            "bucketName": bucket,
            "objectName": object_name,
            "contentType": content_type,
            "cacheControl": "3600",
        },
    )
    uploader.upload()


def processar_fila():
    print("🤖 Robô GitHub Worker Iniciado (Vídeo + Áudio)...")

    # 1) Pegar 1 job PROCESSANDO
    response = (
        supabase.table("reuniao_processing_queue")
        .select("*")
        .eq("status", "PROCESSANDO")
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

    # 2) Trava “quase atômica” (condiciona status)
    #    Evita dois runners processarem o mesmo job.
    lock_res = (
        supabase.table("reuniao_processing_queue")
        .update(
            {
                "status": "PROCESSANDO_GITH",
                "log_text": "GitHub Actions: Renderizando Vídeo e Áudio...",
            }
        )
        .eq("id", job_id)
        .eq("status", "PROCESSANDO")
        .execute()
    )
    if not lock_res.data:
        print("⚠️ Job já foi travado por outro worker. Saindo.")
        return

    local_files = []
    list_file_path = f"list_{reuniao_id}.txt"
    output_video = f"output_{reuniao_id}.mp4"
    output_audio = f"audio_{reuniao_id}.mp3"

    try:
        # A) Listar arquivos
        print("📂 Listando arquivos no Supabase...")
        arquivos_raiz = supabase.storage.from_("gravacoes").list(f"reunioes/{reuniao_id}")
        sessao_folder = next((i["name"] for i in arquivos_raiz if i["name"].startswith("sess_")), None)
        if not sessao_folder:
            raise Exception("Pasta de sessão (sess_*) não encontrada.")

        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
        arquivos = supabase.storage.from_("gravacoes").list(caminho_base)

        partes = [p for p in arquivos if p["name"].startswith("part_") and p["name"].endswith(".webm")]
        partes.sort(key=lambda x: x["name"])
        if not partes:
            raise Exception("Nenhuma parte .webm encontrada.")

        # B) Download + list.txt
        print(f"⬇️ Baixando {len(partes)} partes...")
        with open(list_file_path, "w", encoding="utf-8") as f_list:
            for p in partes:
                local_path = p["name"]  # salva na raiz do runner
                print(f"   - Baixando {p['name']}...")
                data = supabase.storage.from_("gravacoes").download(f"{caminho_base}/{p['name']}")
                with open(local_path, "wb") as f_video:
                    f_video.write(data)

                local_files.append(local_path)
                # ffmpeg concat demuxer
                f_list.write(f"file '{local_path}'\n")

        # C1) Gerar MP4 (30fps) — mantém o “segredo” do -r 30
        print("🎬 Gerando Vídeo MP4 (30fps)...")
        cmd_video = [
            "ffmpeg",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_file_path,
            "-r",
            "30",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "28",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-movflags",
            "+faststart",
            "-y",
            output_video,
        ]
        subprocess.run(cmd_video, check=True)

        video_mb = os.path.getsize(output_video) / (1024 * 1024)
        print(f"✅ Vídeo gerado: {video_mb:.2f} MB")

        # C2) EXTRAÇÃO: MP3 para IA
        print("🎵 Extraindo Áudio MP3 para a IA...")
        cmd_audio = [
            "ffmpeg",
            "-i",
            output_video,
            "-vn",
            "-acodec",
            "libmp3lame",
            "-b:a",
            "32k",
            "-ar",
            "44100",
            "-y",
            output_audio,
        ]
        subprocess.run(cmd_audio, check=True)

        audio_kb = os.path.getsize(output_audio) / 1024
        print(f"✅ Áudio gerado: {audio_kb:.0f} KB")

        # D) Upload Duplo (TUS) — com chunk_size e cacheControl
        tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
        headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"}
        my_client = client.TusClient(url=tus_url, headers=headers)

        path_video = f"{caminho_base}/video_completo_render.mp4"
        path_audio = f"{caminho_base}/audio_executivo.mp3"

        print("⬆️ Uploading Vídeo...")
        tus_upload(my_client, output_video, "gravacoes", path_video, "video/mp4")

        print("⬆️ Uploading Áudio...")
        tus_upload(my_client, output_audio, "gravacoes", path_audio, "audio/mpeg")

        # E) Finalização (DB)
        print("💾 Atualizando Banco de Dados...")
        supabase.table("reunioes").update(
            {
                "gravacao_path": path_video,
                "gravacao_audio_path": path_audio,  # Crucial para Central de Atas
                "gravacao_audio_bucket": "gravacoes",
                "gravacao_status": "CONCLUIDO",
                "gravacao_mime": "video/mp4",
                "gravacao_size_bytes": os.path.getsize(output_video),
            }
        ).eq("id", reuniao_id).execute()

        supabase.table("reuniao_processing_queue").update(
            {"status": "CONCLUIDO", "log_text": f"Sucesso: Vídeo({video_mb:.1f}MB) e Áudio gerados."}
        ).eq("id", job_id).execute()

        # F) Limpeza de partes originais no Supabase (em lotes)
        print("🗑️ Apagando partes originais para liberar espaço...")
        caminhos_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
        for i in range(0, len(caminhos_apagar), BATCH_DELETE_SIZE):
            batch = caminhos_apagar[i : i + BATCH_DELETE_SIZE]
            supabase.storage.from_("gravacoes").remove(batch)
            print(f"   - Lote {i // BATCH_DELETE_SIZE + 1} removido ({len(batch)} itens).")

    except Exception as e:
        print(f"❌ ERRO: {e}")
        supabase.table("reuniao_processing_queue").update(
            {"status": "ERRO", "log_text": str(e)}
        ).eq("id", job_id).execute()
        raise
    finally:
        # Limpeza local
        for f in local_files + [list_file_path, output_video, output_audio]:
            try:
                if f and os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass


if __name__ == "__main__":
    processar_fila()
