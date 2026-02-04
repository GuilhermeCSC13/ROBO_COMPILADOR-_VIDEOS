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

@app.route('/')
def home():
    return "🤖 Robô Pro (Compressor de Reuniões) Ativo."

@app.route('/processar')
def processar():
    # 1. Pegar Job Pendente
    response = supabase.table("reuniao_processing_queue")\
        .select("*").eq("status", "PENDENTE").limit(1).execute()
    
    jobs = response.data
    if not jobs: return jsonify({"status": "Fila vazia"})

    job = jobs[0]
    reuniao_id = job['reuniao_id']
    job_id = job['id']
    print(f"🚀 Iniciando Job: {reuniao_id} (Modo Compressão)")

    # Atualiza status para evitar duplicidade
    supabase.table("reuniao_processing_queue")\
        .update({"status": "PROCESSANDO_RENDER", "log_text": "Baixando e Comprimindo..."})\
        .eq("id", job_id).execute()

    local_files = []
    list_file_path = f"/tmp/{reuniao_id}_list.txt"
    output_compressed = f"/tmp/{reuniao_id}_compressed.mp4"

    try:
        # A. Mapear Arquivos no Storage
        arquivos_raiz = supabase.storage.from_("gravacoes").list(f"reunioes/{reuniao_id}")
        sessao_folder = next((i['name'] for i in arquivos_raiz if i['name'].startswith('sess_')), None)
        
        if not sessao_folder: raise Exception("Pasta sessão não encontrada")
        
        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
        arquivos = supabase.storage.from_("gravacoes").list(caminho_base)
        
        # Filtra apenas partes .webm
        partes = [p for p in arquivos if p['name'].startswith('part_') and p['name'].endswith('.webm')]
        partes.sort(key=lambda x: x['name']) # Garante ordem cronológica
        
        if not partes: raise Exception("Sem partes .webm para processar")

        # B. Download das Partes
        print(f"⬇️ Baixando {len(partes)} partes...")
        with open(list_file_path, 'w') as f_list:
            for p in partes:
                local_path = f"/tmp/{p['name']}"
                with open(local_path, "wb") as f_video:
                    data = supabase.storage.from_("gravacoes").download(f"{caminho_base}/{p['name']}")
                    f_video.write(data)
                local_files.append(local_path)
                # Escreve no arquivo de lista do FFmpeg
                f_list.write(f"file '{local_path}'\n")

        # C. COMPRESSÃO COM FFMPEG (O Segredo da Economia)
        print("⚙️ Comprimindo vídeo (Isso pode demorar)...")
        
        # Comando FFmpeg otimizado para reuniões:
        # -c:v libx264: Codec eficiente
        # -crf 28: Nível de compressão (23 é padrão, 28 é menor tamanho/qualidade ok para reunião)
        # -preset veryfast: Para não estourar o tempo do Render
        # -c:a aac -b:a 64k: Áudio otimizado para voz
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
            "-movflags", "+faststart", # Permite tocar antes de baixar tudo
            "-y",
            output_compressed
        ]
        
        subprocess.run(ffmpeg_cmd, check=True)

        # Verifica tamanho final
        tamanho_mb = os.path.getsize(output_compressed) / (1024 * 1024)
        print(f"✅ Vídeo Comprimido: {tamanho_mb:.2f} MB")

        # D. Upload Seguro (TUS)
        print("⬆️ Subindo vídeo otimizado...")
        path_destino = f"{caminho_base}/video_completo_render.mp4"
        
        tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
        my_client = client.TusClient(url=tus_url, headers={"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"})
        
        uploader = my_client.uploader(
            file_path=output_compressed,
            chunk_size=6 * 1024 * 1024,
            metadata={
                "bucketName": "gravacoes",
                "objectName": path_destino,
                "contentType": "video/mp4",
                "cacheControl": "3600"
            }
        )
        uploader.upload()
        
        # E. Limpeza e Finalização
        print("🧹 Limpando arquivos temporários da nuvem...")
        caminhos_para_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
        
        # Apaga em lotes de 20
        batch_size = 20
        for i in range(0, len(caminhos_para_apagar), batch_size):
            batch = caminhos_para_apagar[i:i + batch_size]
            supabase.storage.from_("gravacoes").remove(batch)

        # Atualiza Banco de Dados
        # Pegamos a duração real do arquivo gerado usando ffprobe (opcional, mas bom pra precisão)
        # Por simplificação, mantemos o update padrão, mas setamos status CONCLUIDO
        supabase.table("reunioes").update({
            "gravacao_path": path_destino,
            "gravacao_status": "CONCLUIDO",
            "gravacao_mime": "video/mp4",
            "gravacao_size_bytes": os.path.getsize(output_compressed)
        }).eq("id", reuniao_id).execute()

        supabase.table("reuniao_processing_queue").update({"status": "CONCLUIDO", "log_text": f"Sucesso. Tamanho: {tamanho_mb:.1f}MB"}).eq("id", job_id).execute()
        
        # Limpeza Local
        if os.path.exists(output_compressed): os.remove(output_compressed)
        if os.path.exists(list_file_path): os.remove(list_file_path)
        for f in local_files:
            if os.path.exists(f): os.remove(f)

        return jsonify({"status": "Sucesso", "id": reuniao_id, "tamanho_mb": tamanho_mb})

    except Exception as e:
        print(f"❌ Erro: {e}")
        supabase.table("reuniao_processing_queue").update({"status": "ERRO", "log_text": str(e)}).eq("id", job_id).execute()
        
        # Tenta limpar lixo local em caso de erro
        if os.path.exists(output_compressed): os.remove(output_compressed)
        return jsonify({"status": "Erro", "msg": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
