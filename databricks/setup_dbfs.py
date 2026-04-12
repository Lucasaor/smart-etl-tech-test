"""Setup DBFS — Upload de dados e configuração inicial no Databricks.

Script para preparar o ambiente Databricks com os dados necessários
para o pipeline. Faz upload de dados brutos e sincronização de specs
do diretório local para o DBFS.

Uso:
    python databricks/setup_dbfs.py --profile DEFAULT
    python databricks/setup_dbfs.py --include-generated

Pré-requisitos:
    - databricks-sdk instalado: pip install databricks-sdk
    - Perfil configurado em ~/.databrickscfg ou variáveis de ambiente:
      DATABRICKS_HOST, DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
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


def upload_if_exists(client, local_path: Path, dbfs_path: str) -> None:
    """Helper de upload que pula arquivos ausentes sem interromper o setup."""
    if not local_path.exists() or not local_path.is_file():
        print(f"  ⚠️  Arquivo não encontrado: {local_path}")
        return
    upload_para_dbfs(client, str(local_path), dbfs_path)


def sync_specs(client, spec_dir: Path, include_generated: bool) -> None:
    """Sincroniza artefatos de spec da pasta local para a pasta de specs no DBFS."""
    print(f"Sincronizando specs de: {spec_dir}")

    upload_if_exists(client, spec_dir / "conversations_bronze.parquet", "/mnt/delta/specs/conversations_bronze.parquet")
    upload_if_exists(client, spec_dir / "dicionario_dados.md", "/mnt/delta/specs/dicionario_dados.md")
    upload_if_exists(client, spec_dir / "descricao_kpis.md", "/mnt/delta/specs/descricao_kpis.md")
    upload_if_exists(client, spec_dir / "spec_meta.json", "/mnt/delta/specs/spec_meta.json")

    if not include_generated:
        return

    generated_dir = spec_dir / "generated"
    if not generated_dir.exists() or not generated_dir.is_dir():
        print(f"  ⚠️  Diretório de gerados não encontrado: {generated_dir}")
        return

    client.dbfs.mkdirs("/mnt/delta/specs/generated")
    print("Sincronizando artefatos gerados...")
    for file_path in sorted(generated_dir.rglob("*")):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(generated_dir).as_posix()
        upload_para_dbfs(client, str(file_path), f"/mnt/delta/specs/generated/{rel}")


def criar_diretorios_dbfs(client) -> None:
    """Cria a estrutura de diretórios no DBFS para o pipeline."""
    diretorios = [
        "/mnt/delta/specs",
        "/mnt/delta/specs/generated",
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
        "/mnt/delta/monitoring/pipeline_runs",
        "/mnt/delta/monitoring/agent_actions",
        "/mnt/delta/monitoring/alerts",
        "/mnt/delta/monitoring/notebook_runs",
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
        default="./data/specs/conversations_bronze.parquet",
        help="Caminho do arquivo parquet de dados brutos",
    )
    parser.add_argument(
        "--dicionario",
        type=str,
        default="./data/specs/dicionario_dados.md",
        help="Caminho do dicionário de dados (markdown)",
    )
    parser.add_argument(
        "--spec-dir",
        type=str,
        default="./data/specs",
        help="Diretório local com specs e artefatos gerados",
    )
    parser.add_argument(
        "--include-generated",
        action="store_true",
        help="Também sincroniza o diretório ./generated para /mnt/delta/specs/generated",
    )
    parser.add_argument(
        "--skip-spec-sync",
        action="store_true",
        help="Pula sincronização do diretório de specs",
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
        print("\nUpload de dados para DBFS...")
        upload_para_dbfs(client, args.source, "/mnt/delta/specs/conversations_bronze.parquet")
        upload_para_dbfs(client, args.dicionario, "/mnt/delta/specs/dicionario_dados.md")

    if args.skip_spec_sync:
        print("\n⏩ Sincronização de specs pulada (--skip-spec-sync)")
    else:
        print("\nSincronização de specs para DBFS...")
        sync_specs(
            client=client,
            spec_dir=Path(args.spec_dir).resolve(),
            include_generated=args.include_generated,
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
    print("     (ou use Repos → Add Repo apontando para o GitHub)")
    print("  2. Execute 01_bronze → 02_silver → 03_gold na ordem")
    print("  3. Ou use 04_agent_orchestrator para execução completa")
    print("  4. Consulte databricks/RUNBOOK.md para detalhes operacionais")


if __name__ == "__main__":
    main()
