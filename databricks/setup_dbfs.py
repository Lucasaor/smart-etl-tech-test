"""Setup DBFS — Upload de dados e configuração inicial no Databricks.

Script para preparar o ambiente Databricks com os dados necessários
para o pipeline. Faz upload do parquet de dados brutos, dicionário
e especificação do projeto para o DBFS.

Uso:
    python databricks/setup_dbfs.py --profile DEFAULT --source ./conversations_bronze.parquet

Pré-requisitos:
    - databricks-sdk instalado: pip install databricks-sdk
    - Perfil configurado em ~/.databrickscfg ou variáveis de ambiente:
      DATABRICKS_HOST, DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def get_workspace_client(profile: str | None = None):
    """Cria um cliente Databricks SDK.

    Args:
        profile: Nome do perfil no ~/.databrickscfg. Se None, usa variáveis de ambiente.

    Returns:
        WorkspaceClient configurado.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERRO: databricks-sdk não instalado.")
        print("Instale com: pip install databricks-sdk")
        sys.exit(1)

    kwargs = {}
    if profile:
        kwargs["profile"] = profile

    return WorkspaceClient(**kwargs)


def upload_para_dbfs(client, local_path: str, dbfs_path: str) -> None:
    """Upload de um arquivo local para o DBFS.

    Args:
        client: WorkspaceClient do Databricks.
        local_path: Caminho do arquivo local.
        dbfs_path: Caminho destino no DBFS.
    """
    local_path = str(Path(local_path).resolve())

    if not Path(local_path).exists():
        print(f"  ⚠️  Arquivo não encontrado: {local_path}")
        return

    tamanho = Path(local_path).stat().st_size
    print(f"  📤 Uploading: {local_path} → {dbfs_path} ({tamanho / 1024:.1f} KB)")

    with open(local_path, "rb") as f:
        client.dbfs.upload(dbfs_path, f, overwrite=True)

    print(f"  ✅ Upload concluído: {dbfs_path}")


def criar_diretorios_dbfs(client) -> None:
    """Cria a estrutura de diretórios no DBFS para o pipeline."""
    diretorios = [
        "/mnt/delta/specs",
        "/mnt/delta/bronze",
        "/mnt/delta/silver",
        "/mnt/delta/silver/messages",
        "/mnt/delta/silver/conversations",
        "/mnt/delta/gold",
        "/mnt/delta/gold/sentiment",
        "/mnt/delta/gold/personas",
        "/mnt/delta/gold/segmentation",
        "/mnt/delta/gold/analytics",
        "/mnt/delta/gold/vendor_analysis",
        "/mnt/delta/monitoring",
    ]

    print("Criando estrutura de diretórios no DBFS...")
    for d in diretorios:
        try:
            client.dbfs.mkdirs(d)
            print(f"  📁 {d}")
        except Exception as e:
            print(f"  ⚠️  {d}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Setup do DBFS para o Agentic Pipeline"
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Perfil do Databricks CLI (padrão: usa variáveis de ambiente)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="./conversations_bronze.parquet",
        help="Caminho do arquivo parquet de dados brutos",
    )
    parser.add_argument(
        "--dicionario",
        type=str,
        default="./Dicionario_de_Dados.md",
        help="Caminho do dicionário de dados (markdown)",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Pular upload de dados (apenas criar diretórios)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Agentic Pipeline — Setup DBFS")
    print("=" * 60)

    # Conectar ao Databricks
    print(f"\nConectando ao Databricks (perfil: {args.profile or 'env vars'})...")
    try:
        client = get_workspace_client(args.profile)
        # Teste de conexão
        clusters = list(client.clusters.list())
        print(f"  ✅ Conectado! ({len(clusters)} cluster(s) encontrado(s))")
    except Exception as e:
        print(f"  ❌ Erro de conexão: {e}")
        print("\nVerifique:")
        print("  1. DATABRICKS_HOST e DATABRICKS_TOKEN estão definidos")
        print("  2. Ou configure um perfil: databricks configure --profile DEFAULT")
        sys.exit(1)

    # Criar diretórios
    print()
    criar_diretorios_dbfs(client)

    if args.skip_upload:
        print("\n⏩ Upload de dados pulado (--skip-upload)")
    else:
        # Upload de dados
        print("\nUpload de dados para DBFS...")

        upload_para_dbfs(
            client,
            args.source,
            "/mnt/delta/specs/conversations_bronze.parquet",
        )

        upload_para_dbfs(
            client,
            args.dicionario,
            "/mnt/delta/specs/Dicionario_de_Dados.md",
        )

    # Verificação final
    print("\n" + "=" * 60)
    print("Verificação dos arquivos no DBFS:")
    print("=" * 60)

    try:
        arquivos = client.dbfs.list("/mnt/delta/specs")
        if arquivos:
            for f in arquivos:
                tamanho = f.file_size or 0
                tipo = "📁" if f.is_dir else "📄"
                print(f"  {tipo} {f.path} ({tamanho / 1024:.1f} KB)")
        else:
            print("  (vazio)")
    except Exception as e:
        print(f"  ⚠️  Erro ao listar: {e}")

    print("\n✅ Setup concluído!")
    print("\nPróximos passos:")
    print("  1. Importe os notebooks de databricks/notebooks/ para o Workspace")
    print("  2. Execute 01_bronze → 02_silver → 03_gold na ordem")
    print("  3. Ou use 04_agent_orchestrator para execução completa")


if __name__ == "__main__":
    main()
