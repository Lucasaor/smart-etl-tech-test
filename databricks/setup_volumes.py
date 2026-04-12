"""Setup UC Volumes — Upload de dados e configuração inicial no Databricks Free Edition.

Script para preparar o ambiente Databricks Free Edition com os dados necessários
para o pipeline. Faz upload de dados brutos e sincronização de specs para
Unity Catalog Volumes.

Uso:
    python databricks/setup_volumes.py
    python databricks/setup_volumes.py --include-generated
    python databricks/setup_volumes.py --catalog my_catalog --schema default --volume pipeline_data

Pré-requisitos:
    - databricks-sdk instalado: pip install databricks-sdk
    - Variáveis de ambiente configuradas:
      DATABRICKS_HOST, DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path, PurePosixPath


# Default UC Volume coordinates
# On Free Edition the default catalog is usually NOT "main".
# Use --catalog to specify, or leave as "auto" for auto-detection.
DEFAULT_CATALOG = "auto"
DEFAULT_SCHEMA = "default"
DEFAULT_VOLUME = "pipeline_data"


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


def detect_catalog(client, requested: str) -> str:
    """Detect the correct catalog name.

    If *requested* is 'auto', queries the workspace for available catalogs
    and picks the first usable one.  If a specific name is given, validates
    that it exists.
    """
    try:
        catalogs = list(client.catalogs.list())
    except Exception as e:
        if requested != "auto":
            return requested            # can't validate; let it fail later
        print(f"  ⚠️  Não foi possível listar catálogos: {e}")
        print("     Especifique o catálogo manualmente com --catalog <nome>")
        sys.exit(1)

    catalog_names = [c.name for c in catalogs if c.name]

    if requested != "auto":
        if requested in catalog_names:
            return requested
        print(f"  ❌ Catálogo '{requested}' não encontrado.")
        print(f"     Catálogos disponíveis: {', '.join(catalog_names)}")
        print(f"     Use: python databricks/setup_volumes.py --catalog <nome>")
        sys.exit(1)

    # Auto-detect: prefer 'main', then first non-system catalog
    if "main" in catalog_names:
        return "main"

    # Filter out system/internal catalogs
    user_catalogs = [n for n in catalog_names if not n.startswith(("__", "system", "hive_metastore"))]
    if len(user_catalogs) == 1:
        print(f"  🔍 Catálogo auto-detectado: {user_catalogs[0]}")
        return user_catalogs[0]

    if user_catalogs:
        print(f"  ⚠️  Múltiplos catálogos encontrados: {', '.join(user_catalogs)}")
        print(f"     Usando o primeiro: {user_catalogs[0]}")
        print(f"     Para escolher outro: --catalog <nome>")
        return user_catalogs[0]

    # Fallback: any catalog
    if catalog_names:
        print(f"  ⚠️  Nenhum catálogo de usuário encontrado. Catálogos disponíveis: {', '.join(catalog_names)}")
        print(f"     Especifique com --catalog <nome>")
        sys.exit(1)

    print("  ❌ Nenhum catálogo encontrado no workspace.")
    sys.exit(1)


def volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    """Build a UC Volume path: /Volumes/<catalog>/<schema>/<volume>/<parts...>"""
    base = PurePosixPath("/Volumes") / catalog / schema / volume
    for p in parts:
        base = base / p
    return str(base)


def upload_to_volume(client, local_path: str, vol_path: str) -> None:
    """Upload de um arquivo local para um UC Volume via Files API.

    Args:
        client: WorkspaceClient do Databricks.
        local_path: Caminho do arquivo local.
        vol_path: Caminho destino no Volume (ex: /Volumes/main/default/pipeline_data/specs/file.parquet).
    """
    local_path = str(Path(local_path).resolve())

    if not Path(local_path).exists():
        print(f"  ⚠️  Arquivo não encontrado: {local_path}")
        return

    tamanho = Path(local_path).stat().st_size
    print(f"  📤 Uploading: {local_path} → {vol_path} ({tamanho / 1024:.1f} KB)")

    with open(local_path, "rb") as f:
        client.files.upload(vol_path, f, overwrite=True)

    print(f"  ✅ Upload concluído: {vol_path}")


def upload_if_exists(client, local_path: Path, vol_path: str) -> None:
    """Upload helper that skips missing files without interrupting the setup."""
    if not local_path.exists() or not local_path.is_file():
        print(f"  ⚠️  Arquivo não encontrado: {local_path}")
        return
    upload_to_volume(client, str(local_path), vol_path)


def sync_specs(client, spec_dir: Path, base_path: str, include_generated: bool) -> None:
    """Sync spec artifacts from local folder to UC Volume specs folder."""
    specs_path = f"{base_path}/specs"
    print(f"Sincronizando specs de: {spec_dir}")

    upload_if_exists(client, spec_dir / "conversations_bronze.parquet", f"{specs_path}/conversations_bronze.parquet")
    upload_if_exists(client, spec_dir / "dicionario_dados.md", f"{specs_path}/dicionario_dados.md")
    upload_if_exists(client, spec_dir / "descricao_kpis.md", f"{specs_path}/descricao_kpis.md")
    upload_if_exists(client, spec_dir / "spec_meta.json", f"{specs_path}/spec_meta.json")

    if not include_generated:
        return

    generated_dir = spec_dir / "generated"
    if not generated_dir.exists() or not generated_dir.is_dir():
        print(f"  ⚠️  Diretório de gerados não encontrado: {generated_dir}")
        return

    print("Sincronizando artefatos gerados...")
    for file_path in sorted(generated_dir.rglob("*")):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(generated_dir).as_posix()
        upload_to_volume(client, str(file_path), f"{specs_path}/generated/{rel}")


def criar_volume_se_necessario(client, catalog: str, schema: str, volume_name: str) -> None:
    """Cria o UC Volume se ele ainda não existir."""
    try:
        client.volumes.read(f"{catalog}.{schema}.{volume_name}")
        print(f"  ✅ Volume {catalog}.{schema}.{volume_name} já existe")
    except Exception:
        try:
            from databricks.sdk.service.catalog import VolumeType

            print(f"  📦 Criando volume {catalog}.{schema}.{volume_name}...")
            client.volumes.create(
                catalog_name=catalog,
                schema_name=schema,
                name=volume_name,
                volume_type=VolumeType.MANAGED,
            )
            print(f"  ✅ Volume criado: {catalog}.{schema}.{volume_name}")
        except Exception as e:
            print(f"  ⚠️  Não foi possível criar volume: {e}")
            print("     O volume pode já existir ou você precisa criar manualmente via UI.")


def criar_diretorios_volume(client, base_path: str) -> None:
    """Cria a estrutura de diretórios no UC Volume para o pipeline.

    On UC Volumes, directories are created implicitly when files are uploaded.
    We upload a .gitkeep placeholder to ensure the directories exist.
    """
    diretorios = [
        "specs",
        "specs/generated",
        "specs/generated/tests",
        "bronze",
        "silver",
        "silver/messages",
        "silver/conversations",
        "gold",
        "gold/sentiment",
        "gold/personas",
        "gold/segmentation",
        "gold/analytics",
        "gold/vendor_analysis",
        "monitoring",
        "monitoring/pipeline_runs",
        "monitoring/agent_actions",
        "monitoring/alerts",
        "monitoring/notebook_runs",
    ]

    print("Criando estrutura de diretórios no UC Volume...")
    for d in diretorios:
        try:
            dir_path = f"{base_path}/{d}"
            client.files.create_directory(dir_path)
            print(f"  📁 {dir_path}")
        except Exception:
            # Directory may already exist or be created implicitly
            print(f"  📁 {dir_path} (ok)")


def main():
    parser = argparse.ArgumentParser(
        description="Setup de UC Volumes para o Agentic Pipeline (Databricks Free Edition)"
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Perfil do Databricks CLI (padrão: usa variáveis de ambiente)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CATALOG,
        help=f"Nome do catálogo UC (padrão: auto-detecta)",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=DEFAULT_SCHEMA,
        help=f"Nome do schema UC (padrão: {DEFAULT_SCHEMA})",
    )
    parser.add_argument(
        "--volume",
        type=str,
        default=DEFAULT_VOLUME,
        help=f"Nome do volume UC (padrão: {DEFAULT_VOLUME})",
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
        help="Também sincroniza o diretório ./generated para o Volume",
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
    print("Agentic Pipeline — Setup UC Volumes (Free Edition)")
    print("=" * 60)

    # Conectar ao Databricks
    print(f"\nConectando ao Databricks (perfil: {args.profile or 'env vars'})...")
    try:
        client = get_workspace_client(args.profile)
        # Teste de conexão
        current_user = client.current_user.me()
        print(f"  ✅ Conectado como {current_user.user_name}")
    except Exception as e:
        print(f"  ❌ Erro de conexão: {e}")
        print("\nVerifique:")
        print("  1. DATABRICKS_HOST e DATABRICKS_TOKEN estão definidos")
        print("  2. Ou configure um perfil: databricks configure --profile DEFAULT")
        sys.exit(1)

    # Detect / validate catalog
    catalog = detect_catalog(client, args.catalog)
    if catalog != args.catalog:
        print(f"  📦 Catálogo resolvido: {catalog}")
    args.catalog = catalog
    base_path = volume_path(args.catalog, args.schema, args.volume)
    print(f"  📦 Volume path: {base_path}")

    # Criar volume se necessário
    print()
    criar_volume_se_necessario(client, args.catalog, args.schema, args.volume)

    # Criar diretórios
    print()
    criar_diretorios_volume(client, base_path)

    if args.skip_upload:
        print("\n⏩ Upload de dados pulado (--skip-upload)")
    else:
        print("\nUpload de dados para UC Volume...")
        upload_to_volume(client, args.source, f"{base_path}/specs/conversations_bronze.parquet")
        upload_to_volume(client, args.dicionario, f"{base_path}/specs/dicionario_dados.md")

    if args.skip_spec_sync:
        print("\n⏩ Sincronização de specs pulada (--skip-spec-sync)")
    else:
        print("\nSincronização de specs para UC Volume...")
        sync_specs(
            client=client,
            spec_dir=Path(args.spec_dir).resolve(),
            base_path=base_path,
            include_generated=args.include_generated,
        )

    # Verificação final
    print("\n" + "=" * 60)
    print("Verificação dos arquivos no UC Volume:")
    print("=" * 60)

    try:
        items = client.files.list_directory_contents(f"{base_path}/specs")
        for item in items:
            tamanho = getattr(item, "file_size", 0) or 0
            is_dir = getattr(item, "is_directory", False)
            tipo = "📁" if is_dir else "📄"
            print(f"  {tipo} {item.path} ({tamanho / 1024:.1f} KB)")
    except Exception as e:
        print(f"  ⚠️  Erro ao listar: {e}")

    print("\n✅ Setup concluído!")
    print("\nPróximos passos:")
    print("  1. Importe os notebooks de databricks/notebooks/ para o Workspace")
    print("     (ou use Repos → Add Repo apontando para o GitHub)")
    print("  2. Execute 05_agentic_pipeline.py (recomendado) ou 01-04 em sequência")
    print("  3. Consulte databricks/RUNBOOK.md para detalhes operacionais")
    print(f"\n  Volume path para notebooks: {base_path}")


if __name__ == "__main__":
    main()
