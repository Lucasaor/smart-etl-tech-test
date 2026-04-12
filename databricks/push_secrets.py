"""Enviar valores do .env para Databricks Secrets.

Lê o arquivo .env local, sobrescreve RUNTIME_ENV para "databricks"
e armazena cada par chave/valor como um secret no Databricks.

Uso:
    python databricks/push_secrets.py
    python databricks/push_secrets.py --env-file .env --scope pipeline
    python databricks/push_secrets.py --dry-run          # prévia sem enviar
    python databricks/push_secrets.py --profile DEFAULT   # usar perfil CLI

Pré-requisitos:
    - databricks-sdk instalado: pip install databricks-sdk
    - Variáveis de ambiente DATABRICKS_HOST / DATABRICKS_TOKEN (ou --profile)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DEFAULT_SCOPE = "pipeline"


# ── helpers ──────────────────────────────────────────────────────────────

def get_workspace_client(profile: str | None = None):
    """Cria um WorkspaceClient do Databricks."""
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


def parse_env_file(env_path: str) -> dict[str, str]:
    """Faz o parse de um arquivo .env para um dict, ignorando comentários e linhas em branco.

    Trata aspas opcionais (simples/duplas) ao redor dos valores.
    """
    entries: dict[str, str] = {}
    path = Path(env_path).resolve()

    if not path.exists():
        print(f"❌ Arquivo .env não encontrado: {path}")
        sys.exit(1)

    with open(path) as f:
        for line_no, raw_line in enumerate(f, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)", line)
            if not match:
                continue

            key = match.group(1)
            value = match.group(2).strip()

            # Remover aspas ao redor se presentes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            entries[key] = value

    return entries


def ensure_scope(client, scope_name: str) -> None:
    """Cria o secret scope se ainda não existir."""
    try:
        scopes = client.secrets.list_scopes()
        if any(s.name == scope_name for s in scopes):
            print(f"  ✅ Scope '{scope_name}' já existe")
            return
    except Exception:
        pass  # list may fail; try creating directly

    try:
        client.secrets.create_scope(scope=scope_name)
        print(f"  ✅ Scope '{scope_name}' criado")
    except Exception as e:
        err = str(e)
        if "RESOURCE_ALREADY_EXISTS" in err or "already exists" in err.lower():
            print(f"  ✅ Scope '{scope_name}' já existe")
        else:
            print(f"  ❌ Erro ao criar scope: {e}")
            sys.exit(1)


# ── main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Envia valores do .env para Databricks Secrets (RUNTIME_ENV → 'databricks')"
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Caminho do arquivo .env (padrão: .env)",
    )
    parser.add_argument(
        "--scope",
        type=str,
        default=DEFAULT_SCOPE,
        help=f"Nome do secret scope (padrão: {DEFAULT_SCOPE})",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Perfil do Databricks CLI (padrão: usa variáveis de ambiente)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas exibe o que seria enviado, sem alterar nada",
    )
    parser.add_argument(
        "--skip-empty",
        action="store_true",
        default=True,
        help="Pular variáveis com valor vazio (padrão: True)",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Incluir variáveis com valor vazio",
    )

    args = parser.parse_args()
    skip_empty = not args.include_empty

    # Parse .env
    env_path = Path(args.env_file).resolve()
    print("=" * 60)
    print("Agentic Pipeline — Push Secrets to Databricks")
    print("=" * 60)
    print(f"\n📄 Lendo: {env_path}")

    entries = parse_env_file(str(env_path))

    if not entries:
        print("❌ Nenhuma variável encontrada no .env")
        sys.exit(1)

    # Override RUNTIME_ENV → "databricks"
    original_runtime = entries.get("RUNTIME_ENV", "<não definido>")
    entries["RUNTIME_ENV"] = "databricks"

    print(f"  Variáveis encontradas: {len(entries)}")
    print(f"  RUNTIME_ENV: '{original_runtime}' → 'databricks' (override)")

    # Filter empty values
    if skip_empty:
        before = len(entries)
        entries = {k: v for k, v in entries.items() if v}
        skipped = before - len(entries)
        if skipped:
            print(f"  Variáveis vazias ignoradas: {skipped}")

    # Exclude Databricks credentials from secrets (they're already configured)
    excluded_keys = {"DATABRICKS_HOST", "DATABRICKS_TOKEN"}
    excluded = {k for k in entries if k in excluded_keys}
    for k in excluded:
        del entries[k]
    if excluded:
        print(f"  Excluídas (credenciais Databricks): {', '.join(sorted(excluded))}")

    print(f"\n  Total de secrets a enviar: {len(entries)}")
    print(f"  Scope: {args.scope}")

    # Display summary
    print(f"\n{'─' * 60}")
    print(f"  {'KEY':<35} {'VALUE (masked)'}")
    print(f"  {'─' * 35} {'─' * 20}")
    for key, value in sorted(entries.items()):
        if any(s in key.upper() for s in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
            masked = value[:4] + "..." + value[-4:] if len(value) > 12 else "****"
        else:
            masked = value if len(value) <= 50 else value[:47] + "..."
        print(f"  {key:<35} {masked}")
    print(f"{'─' * 60}")

    if args.dry_run:
        print("\n🔍 DRY RUN — nenhum secret foi enviado")
        print("   Remova --dry-run para executar de verdade")
        return

    # Connect and push
    print(f"\nConectando ao Databricks (perfil: {args.profile or 'env vars'})...")
    client = get_workspace_client(args.profile)

    try:
        user = client.current_user.me()
        print(f"  ✅ Conectado como {user.user_name}")
    except Exception as e:
        print(f"  ❌ Erro de conexão: {e}")
        sys.exit(1)

    # Ensure scope exists
    print()
    ensure_scope(client, args.scope)

    # Push each secret
    print(f"\nEnviando {len(entries)} secrets para scope '{args.scope}'...")
    success = 0
    failed = 0
    for key, value in sorted(entries.items()):
        try:
            client.secrets.put_secret(
                scope=args.scope,
                key=key,
                string_value=value,
            )
            print(f"  ✅ {key}")
            success += 1
        except Exception as e:
            print(f"  ❌ {key}: {e}")
            failed += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(f"✅ Concluído: {success} secrets enviados, {failed} falhas")
    print(f"\nPara usar nos notebooks:")
    print(f"  import os")
    print(f"  os.environ['KEY'] = dbutils.secrets.get('{args.scope}', 'KEY')")
    print(f"\nExemplo completo:")
    print(f"  # Carregar todas as configs do pipeline")
    print(f"  scope = '{args.scope}'")
    print(f"  for key in [s.key for s in dbutils.secrets.list(scope)]:")
    print(f"      os.environ[key] = dbutils.secrets.get(scope, key)")


if __name__ == "__main__":
    main()
