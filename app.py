import os
from flask import Flask, jsonify
from supabase import create_client

app = Flask(__name__)

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️  ERRO: Variáveis de ambiente não encontradas.")

# Conexão
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route('/')
def home():
    return "🤖 Robô de Vídeo Online. Acesse /processar"

@app.route('/processar')
def processar():
    # 1. Pega 1 item da fila
    response = supabase.table("reuniao_processing_queue")\
        .select("*").eq("status", "PENDENTE").limit(1).execute()
    
    jobs = response.data
    if not jobs:
        return jsonify({"status": "Fila vazia", "msg": "Nada para fazer."})

    job = jobs[0]
    reuniao_id = job['reuniao_id']
    job_id = job['id']
    
    print(f"🚀 Processando: {reuniao_id}")

    # 2. Marca como rodando
    supabase.table("reuniao_processing_queue")\
        .update({"status": "PROCESSANDO_RENDER", "log_text": "Iniciando..."})\
        .eq("id", job_id).execute()

    try:
        # A. Achar pasta da sessão
        arquivos_raiz = supabase.storage.from_("gravacoes").list(f"reunioes/{reuniao_id}")
        sessao_folder = next((i['name'] for i in arquivos_raiz if i['name'].startswith('sess_')), None)
        
        if not sessao_folder: raise Exception("Pasta 'sess_' não encontrada")

        caminho_base = f"reunioes/{reuniao_id}/{sessao_folder}"

        # B. Listar partes
        arquivos = supabase.storage.from_("gravacoes").list(caminho_base)
        partes = [p for p in arquivos if p['name'].startswith('part_') and p['name'].endswith('.webm')]
        partes.sort(key=lambda x: x['name'])
        
        if not partes: raise Exception("Nenhuma parte .webm encontrada")

        # C. Baixar e Colar
        nome_temp = f"/tmp/{reuniao_id}.webm"
        with open(nome_temp, "wb") as arquivo_final:
            for p in partes:
                data = supabase.storage.from_("gravacoes").download(f"{caminho_base}/{p['name']}")
                arquivo_final.write(data)

        # D. Upload
        path_destino = f"{caminho_base}/video_completo_render.webm"
        with open(nome_temp, "rb") as f:
            supabase.storage.from_("gravacoes").upload(path_destino, f, {"content-type": "video/webm", "upsert": "true"})

        # E. Atualizar Banco (Força 2h de duração para garantir player livre)
        supabase.table("reunioes").update({
            "gravacao_path": path_destino,
            "gravacao_status": "CONCLUIDO",
            "duracao_segundos": 7200 
        }).eq("id", reuniao_id).execute()

        # F. Finalizar Job
        supabase.table("reuniao_processing_queue").update({"status": "CONCLUIDO"}).eq("id", job_id).execute()
        
        if os.path.exists(nome_temp): os.remove(nome_temp)

        return jsonify({"status": "Sucesso", "id": reuniao_id})

    except Exception as e:
        supabase.table("reuniao_processing_queue").update({"status": "ERRO", "log_text": str(e)}).eq("id", job_id).execute()
        return jsonify({"status": "Erro", "msg": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
