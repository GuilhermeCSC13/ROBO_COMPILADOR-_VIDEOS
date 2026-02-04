import os
import time
import subprocess
from supabase import create_client
from tusclient import client

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("⚠️ Variáveis de ambiente SUPABASE_URL e SUPABASE_KEY são obrigatórias.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def processar_fila():
    print("🤖 Robô GitHub Worker Iniciado...")
    
    # 1. Pegar Job Pendente
    response = supabase.table("reuniao_processing_queue")\
        .select("*").eq("status", "PENDENTE").limit(1).execute()
    
    jobs = response.data
    if not jobs: 
        print("zzZ Fila vazia. Nada para fazer.")
        return

    job = jobs[0]
    reuniao_id = job['reuniao_id']
    job_id = job['id']
    print(f"🚀 Processando Reunião: {reuniao_id}")

    # Trava o job
    supabase.table("reuniao_processing_queue")\
        .update({"status": "PROCESSANDO_RENDER", "log_text": "GitHub Actions: Iniciando..."})\
        .eq("id", job_id).execute()

    local_files = []
    list_file_path = f"list_{reuniao_id}.txt"
    output_compressed = f"output_{reuniao_id}.mp4"

    try:
        # A. Listar Arquivos
        print("📂 Listando arquivos no Supabase...")
        arquivos_raiz = supabase.storage.from_("gravacoes").list(f"reunioes/{reuniao_id}")
        sessao_folder = next((i['name'] for i in arquivos_raiz if i['name'].startswith('sess_')), None)
        
        if not sessao_folder: raise Exception("Pasta de sessão (sess_*) não encontrada.")
        
        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
        arquivos = supabase.storage.from_("gravacoes").list(caminho_base)
        
        partes = [p for p in arquivos if p['name'].startswith('part_') and p['name'].endswith('.webm')]
        partes.sort(key=lambda x: x['name'])
        
        if not partes: raise Exception("Nenhuma parte .webm encontrada.")

        # B. Download
        print(f"⬇️ Baixando {len(partes)} partes...")
        with open(list_file_path, 'w') as f_list:
            for p in partes:
                local_path = p['name'] # Salva na raiz do runner
                print(f"   - Baixando {p['name']}...")
                with open(local_path, "wb") as f_video:
                    data = supabase.storage.from_("gravacoes").download(f"{caminho_base}/{p['name']}")
                    f_video.write(data)
                local_files.append(local_path)
                f_list.write(f"file '{local_path}'\n")

        # C. Compressão FFmpeg
        print("🎬 Iniciando Compressão (FFmpeg)...")
        # Ajuste fino para velocidade vs qualidade
        cmd = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file_path,
            "-c:v", "libx264", "-preset", "faster", "-crf", "28",
            "-c:a", "aac", "-b:a", "64k",
            "-movflags", "+faststart", "-y", output_compressed
        ]
        subprocess.run(cmd, check=True)
        
        tamanho_mb = os.path.getsize(output_compressed) / (1024 * 1024)
        print(f"✅ Vídeo gerado com sucesso: {tamanho_mb:.2f} MB")

        # D. Upload
        print("⬆️ Realizando Upload...")
        path_destino = f"{caminho_base}/video_completo_render.mp4"
        
        tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
        my_client = client.TusClient(url=tus_url, headers={"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"})
        
        uploader = my_client.uploader(
            file_path=output_compressed,
            chunk_size=50 * 1024 * 1024, # 50MB chunks (GitHub tem internet rapida)
            metadata={
                "bucketName": "gravacoes",
                "objectName": path_destino,
                "contentType": "video/mp4",
                "cacheControl": "3600"
            }
        )
        uploader.upload()

        # E. Finalização
        print("💾 Atualizando Banco de Dados...")
        supabase.table("reunioes").update({
            "gravacao_path": path_destino,
            "gravacao_status": "CONCLUIDO",
            "gravacao_mime": "video/mp4",
            "gravacao_size_bytes": os.path.getsize(output_compressed)
        }).eq("id", reuniao_id).execute()

        supabase.table("reuniao_processing_queue").update({
            "status": "CONCLUIDO", 
            "log_text": f"Sucesso via GitHub Actions. Tamanho: {tamanho_mb:.1f}MB"
        }).eq("id", job_id).execute()

        # Limpeza na Nuvem (Opcional, descomente se quiser limpar as partes)
        # caminhos_para_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
        # if caminhos_para_apagar:
        #     supabase.storage.from_("gravacoes").remove(caminhos_para_apagar)

    except Exception as e:
        print(f"❌ ERRO FATAL: {e}")
        supabase.table("reuniao_processing_queue").update({
            "status": "ERRO", 
            "log_text": str(e)
        }).eq("id", job_id).execute()
        exit(1) # Faz o Action falhar visualmente

if __name__ == "__main__":
    processar_fila()
