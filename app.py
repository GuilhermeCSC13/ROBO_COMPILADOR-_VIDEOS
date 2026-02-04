import os
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
    return "🤖 Robô Pro (Upload Seguro + Limpeza) Ativo."

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
    print(f"🚀 Iniciando Job: {reuniao_id}")

    supabase.table("reuniao_processing_queue")\
        .update({"status": "PROCESSANDO_RENDER", "log_text": "Baixando partes..."})\
        .eq("id", job_id).execute()

    try:
        # A. Mapear Arquivos
        arquivos_raiz = supabase.storage.from_("gravacoes").list(f"reunioes/{reuniao_id}")
        sessao_folder = next((i['name'] for i in arquivos_raiz if i['name'].startswith('sess_')), None)
        
        if not sessao_folder: raise Exception("Pasta sessão não encontrada")
        
        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"
        arquivos = supabase.storage.from_("gravacoes").list(caminho_base)
        partes = [p for p in arquivos if p['name'].startswith('part_') and p['name'].endswith('.webm')]
        partes.sort(key=lambda x: x['name'])
        
        if not partes: raise Exception("Sem partes .webm")

        # B. Download e Fusão Local
        nome_temp = f"/tmp/{reuniao_id}.webm"
        with open(nome_temp, "wb") as arquivo_final:
            for i, p in enumerate(partes):
                print(f"⬇️ Baixando {i+1}/{len(partes)}...")
                data = supabase.storage.from_("gravacoes").download(f"{caminho_base}/{p['name']}")
                arquivo_final.write(data)

        # C. Upload Seguro (TUS)
        print("⬆️ Subindo vídeo completo...")
        path_destino = f"{caminho_base}/video_completo_render.webm"
        
        tus_url = f"{SUPABASE_URL}/storage/v1/upload/resumable"
        my_client = client.TusClient(url=tus_url, headers={"Authorization": f"Bearer {SUPABASE_KEY}", "x-upsert": "true"})
        
        uploader = my_client.uploader(
            file_path=nome_temp,
            chunk_size=6 * 1024 * 1024,
            metadata={
                "bucketName": "gravacoes",
                "objectName": path_destino,
                "contentType": "video/webm",
                "cacheControl": "3600"
            }
        )
        uploader.upload()
        
        # --- PONTO DE SUCESSO ---
        # Se chegou aqui, o upload funcionou. Agora podemos apagar as partes.
        
        print("✅ Upload Sucesso. Iniciando limpeza das partes...")
        caminhos_para_apagar = [f"{caminho_base}/{p['name']}" for p in partes]
        
        # Apaga em lotes de 20
        batch_size = 20
        for i in range(0, len(caminhos_para_apagar), batch_size):
            batch = caminhos_para_apagar[i:i + batch_size]
            supabase.storage.from_("gravacoes").remove(batch)
            print(f"🗑️ Lote {i} removido.")

        # D. Atualizar Banco
        supabase.table("reunioes").update({
            "gravacao_path": path_destino,
            "gravacao_status": "CONCLUIDO",
            "duracao_segundos": 7200 # Ajuste conforme necessário
        }).eq("id", reuniao_id).execute()

        supabase.table("reuniao_processing_queue").update({"status": "CONCLUIDO", "log_text": "Sucesso e Limpeza Completa"}).eq("id", job_id).execute()
        
        if os.path.exists(nome_temp): os.remove(nome_temp)
        return jsonify({"status": "Sucesso", "id": reuniao_id})

    except Exception as e:
        print(f"❌ Erro: {e}")
        supabase.table("reuniao_processing_queue").update({"status": "ERRO", "log_text": str(e)}).eq("id", job_id).execute()
        return jsonify({"status": "Erro", "msg": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
