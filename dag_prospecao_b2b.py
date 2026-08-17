from __future__ import annotations

import sys
from pathlib import Path

# Adiciona o diretório da raiz do projeto ao sys.path do Python
sys.path.append(str(Path(__file__).resolve().parent))


import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.api_clients import (
    Empresa,
    buscar_dados_cnpj,
    buscar_resultados_web_serper,
    enriquecer_emails_kipflow,
    extrair_empresas_e_decisores,
    montar_contexto_web_para_prompt,
    raspar_perfis_linkedin_apify,
)

DEFAULT_ARGS = {
    "owner": "growth-ops",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _validar_identificador_sql(value: str, label: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"{label} inválido para identificador SQL: {value}")
    return value


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "sim"}


def _mascarar_valor(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        upper = v.upper()
        if any(token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD", "API")):
            return "***MASKED***"
        return v
    return v


def _debug_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return {key: _mascarar_valor(value) for key, value in config.items()}


def _render_prompt_template(template: str, params: dict[str, Any]) -> str:
    # Preserva placeholders conhecidos e escapa o restante para suportar JSON sem escape.
    protected = template
    markers: dict[str, str] = {}
    for key in params:
        marker = f"__VAR_{key.upper()}__"
        markers[key] = marker
        protected = protected.replace("{" + key + "}", marker)

    protected = protected.replace("{", "{{").replace("}", "}}")

    for key, marker in markers.items():
        protected = protected.replace(marker, "{" + key + "}")

    return protected.format(**params)


@dag(
    dag_id="prospeccao_b2b",
    description="Pipeline de prospecção B2B com Gemini, minhareceita.org, Apify e Kipflow",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["prospeccao", "b2b", "linkedin"],
)
def dag_prospeccao_b2b():
    @task
    def carregar_configuracoes() -> dict[str, Any]:
        prompt_template = Variable.get(
            "PROSPECCAO_PROMPT_TEMPLATE",
            default_var=(
                "Atue como especialista em pesquisa B2B.\n\n"
                "OBJETIVO\n"
                "Localize empresas B2B no Brasil conforme os parâmetros e identifique seus "
                "decisores via web search e LinkedIn.\n\n"
                "PARAMETROS\n"
                "- Segmento: {segmento_empresa}\n"
                "- Porte: {tamanho_empresa}\n"
                "- Localizacao: {localizacao_empresa}\n"
                "- Total Empresas: {qtd_empresas}\n"
                "- Decisores por Empresa: {qtd_decisores}\n"
                "- Cargos Decisores: {cargos_decisores}\n\n"
                "REGRAS DE BUSCA\n"
                "1. Retorne exatamente {qtd_empresas} empresas B2B em \"{localizacao_empresa}\" "
                "do segmento \"{segmento_empresa}\" e porte \"{tamanho_empresa}\".\n"
                "2. Priorize empresas com site e LinkedIn ativos.\n"
                "3. Para cada empresa, localize ate {qtd_decisores} decisores nos cargos: "
                "{cargos_decisores}.\n"
                "4. Capture URLs diretas do LinkedIn da empresa e dos decisores.\n\n"
                "FORMATO DE SAIDA\n"
                "Retorne EXCLUSIVAMENTE um JSON valido conforme o schema. Sem markdown "
                "(sem ```json), sem texto adicional. Proibido incluir chaves extras fora do "
                "schema.\n\n"
                "SCHEMA JSON\n"
                "{{\n"
                "  \"empresas\": [\n"
                "    {{\n"
                "      \"nome_empresa\": \"string\",\n"
                "      \"site_oficial\": \"string\",\n"
                "      \"linkedin_empresa_url\": \"string\",\n"
                "      \"segmento\": \"string\",\n"
                "      \"porte\": \"string\",\n"
                "      \"cidade\": \"string\",\n"
                "      \"estado\": \"string\",\n"
                "      \"decisores\": [\n"
                "        {{\n"
                "          \"nome\": \"string\",\n"
                "          \"cargo\": \"string\",\n"
                "          \"linkedin_url\": \"string\"\n"
                "        }}\n"
                "      ]\n"
                "    }}\n"
                "  ]\n"
                "}}"
            ),
        )

        segmento_empresa = Variable.get("PROSPECCAO_SEGMENTO_EMPRESA", default_var="tecnologia")
        tamanho_empresa = Variable.get("PROSPECCAO_TAMANHO_EMPRESA", default_var="medio porte")
        localizacao_empresa = Variable.get("PROSPECCAO_LOCALIZACAO_EMPRESA", default_var="Brasil")
        qtd_empresas = int(Variable.get("PROSPECCAO_QTD_EMPRESAS", default_var="10"))
        qtd_decisores = int(Variable.get("PROSPECCAO_QTD_DECISORES", default_var="3"))
        cargos_decisores = Variable.get(
            "PROSPECCAO_CARGOS_DECISORES",
            default_var="CEO, CTO, Head de Vendas, Diretor Comercial",
        )

        # Google GenAI Interactions API: usar Gemini 3.5 Flash Lite como modelo principal.
        modelo = Variable.get("GEMINI_MODEL", default_var="gemini-3.5-flash-lite")
        usar_google_search_tool_raw = Variable.get("USE_GOOGLE_SEARCH_TOOL", default_var="false")
        if usar_google_search_tool_raw.strip().lower() == "auto":
            usar_google_search_tool = not modelo.strip().lower().startswith("gemma")
        else:
            usar_google_search_tool = _to_bool(usar_google_search_tool_raw)

        web_search_provider = Variable.get("WEB_SEARCH_PROVIDER", default_var="serper")
        serper_api_key = Variable.get("SERPER_API_KEY", default_var="")
        serper_num_results = int(Variable.get("SERPER_NUM_RESULTS", default_var="5"))

        prompt_params = {
            "segmento_empresa": segmento_empresa,
            "tamanho_empresa": tamanho_empresa,
            "localizacao_empresa": localizacao_empresa,
            "qtd_empresas": qtd_empresas,
            "qtd_decisores": qtd_decisores,
            "cargos_decisores": cargos_decisores,
        }
        prompt_busca = _render_prompt_template(prompt_template, prompt_params)

        return {
            "prompt_busca": prompt_busca,
            "prompt_template": prompt_template,
            "segmento_empresa": segmento_empresa,
            "tamanho_empresa": tamanho_empresa,
            "localizacao_empresa": localizacao_empresa,
            "qtd_empresas": qtd_empresas,
            "qtd_decisores": qtd_decisores,
            "cargos_decisores": cargos_decisores,
            "usar_google_search_tool": usar_google_search_tool,
            "web_search_provider": web_search_provider,
            "serper_api_key": serper_api_key,
            "serper_num_results": serper_num_results,
            "gemini_api_key": Variable.get("GEMINI_API_KEY"),
            "gemini_model": modelo,
            "apify_api_token": Variable.get("APIFY_API_TOKEN"),
            "linkedin_actor_id": Variable.get(
                "APIFY_LINKEDIN_ACTOR_ID",
                default_var="harvestapi~linkedin-profile-scraper",
            ),
            "kipflow_api_key": Variable.get("KIPFLOW_API_KEY"),
            "kipflow_base_url": Variable.get(
                "KIPFLOW_BASE_URL",
                default_var="https://api.kipflow.com",
            ),
            "cnpj_base_url": Variable.get(
                "CNPJ_BASE_URL",
                default_var="https://minhareceita.org",
            ),
            "postgres_conn_id": Variable.get(
                "POSTGRES_CONN_ID",
                default_var="postgres_default",
            ),
            "postgres_schema": Variable.get(
                "POSTGRES_SCHEMA",
                default_var="public",
            ),
            "postgres_table": Variable.get(
                "POSTGRES_TABLE",
                default_var="prospeccao_b2b_runs",
            ),
        }

    @task
    def coletar_contexto_web(config: dict[str, Any]) -> str:
        if config["usar_google_search_tool"]:
            return ""

        provider = config["web_search_provider"].strip().lower()
        if provider != "serper":
            return ""

        if not config["serper_api_key"]:
            return ""

        queries = [
            (
                f"empresas B2B {config['segmento_empresa']} {config['tamanho_empresa']} "
                f"{config['localizacao_empresa']}"
            ),
            (
                f"site:linkedin.com/company {config['segmento_empresa']} "
                f"{config['localizacao_empresa']}"
            ),
            (
                f"{config['segmento_empresa']} {config['localizacao_empresa']} "
                f"decisor {config['cargos_decisores']}"
            ),
        ]

        resultados = buscar_resultados_web_serper(
            queries=queries,
            serper_api_key=config["serper_api_key"],
            num_results=config["serper_num_results"],
        )
        response = montar_contexto_web_para_prompt(resultados=resultados)
        print("Contexto web coletado para prompt", response)
        return response

    @task
    def extrair_leads_brutos(config: dict[str, Any], contexto_web: str) -> list[dict[str, Any]]:
        # logger.warning("=== DEBUG DAG CONFIG ===")
        # logger.warning(json.dumps(_debug_snapshot(config), ensure_ascii=False, indent=2))
        # logger.warning("=== DEBUG CONTEXTO_WEB ===")
        # logger.warning(contexto_web if contexto_web else "<vazio>")
        # logger.warning("=== DEBUG PROMPT BUSCA ===")
        # logger.warning(config["prompt_busca"])

        empresas = extrair_empresas_e_decisores(
            prompt_busca=config["prompt_busca"],
            gemini_api_key=config["gemini_api_key"],
            model=config["gemini_model"],
            usar_google_search_tool=config["usar_google_search_tool"],
            contexto_web=contexto_web,
        )
        return [empresa.model_dump(mode="json") for empresa in empresas]

    @task
    def enriquecer_dados_empresa(empresa_raw: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        empresa = Empresa.model_validate(empresa_raw)
        if empresa.cnpj:
            empresa.dados_cadastrais = buscar_dados_cnpj(
                cnpj=empresa.cnpj,
                brasil_api_base_url=config["cnpj_base_url"],
            )
        return empresa.model_dump(mode="json")

    @task
    def raspar_linkedin_decisores(empresa_raw: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        empresa = Empresa.model_validate(empresa_raw)

        urls = [
            decisor.linkedin_url
            for decisor in empresa.decisores
            if decisor.linkedin_url and decisor.linkedin_url.strip()
        ]

        perfis = raspar_perfis_linkedin_apify(
            linkedin_urls=urls,
            apify_api_token=config["apify_api_token"],
            actor_id=config["linkedin_actor_id"],
        )

        empresa_dict = empresa.model_dump(mode="json")
        empresa_dict["perfis_linkedin"] = perfis
        return empresa_dict

    @task
    def enriquecer_emails(empresa_raw: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        empresa = Empresa.model_validate(empresa_raw)

        linkedin_ids = [
            decisor.linkedin_id
            for decisor in empresa.decisores
            if decisor.linkedin_id and decisor.linkedin_id.strip()
        ]

        contatos = enriquecer_emails_kipflow(
            linkedin_ids=linkedin_ids,
            kipflow_api_key=config["kipflow_api_key"],
            base_url=config["kipflow_base_url"],
        )

        empresa_dict = empresa.model_dump(mode="json")
        empresa_dict["contatos_enriquecidos"] = contatos
        return empresa_dict

    @task
    def consolidar_saida(empresas: list[dict[str, Any]]) -> dict[str, Any]:
        total_empresas = len(empresas)
        total_contatos = sum(
            len(empresa.get("contatos_enriquecidos", []))
            for empresa in empresas
        )

        return {
            "executado_em": datetime.utcnow().isoformat(),
            "total_empresas": total_empresas,
            "total_contatos_enriquecidos": total_contatos,
            "empresas": empresas,
        }

    @task
    def persistir_saida_postgres(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        schema = _validar_identificador_sql(config["postgres_schema"], "POSTGRES_SCHEMA")
        table = _validar_identificador_sql(config["postgres_table"], "POSTGRES_TABLE")

        hook = PostgresHook(postgres_conn_id=config["postgres_conn_id"])
        create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema}";'
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                total_empresas INTEGER NOT NULL,
                total_contatos_enriquecidos INTEGER NOT NULL,
                payload JSONB NOT NULL
            );
        '''
        insert_sql = f'''
            INSERT INTO "{schema}"."{table}" (
                total_empresas,
                total_contatos_enriquecidos,
                payload
            ) VALUES (%s, %s, %s::jsonb);
        '''

        hook.run([create_schema_sql, create_table_sql])
        hook.run(
            insert_sql,
            parameters=(
                payload.get("total_empresas", 0),
                payload.get("total_contatos_enriquecidos", 0),
                json.dumps(payload, ensure_ascii=False),
            ),
        )

        return {
            "postgres_conn_id": config["postgres_conn_id"],
            "postgres_schema": schema,
            "postgres_table": table,
            "status": "persistido",
        }

    @task
    def imprimir_resumo(payload: dict[str, Any], persistencia: dict[str, Any]) -> None:
        print(
            json.dumps(
                {
                    "resumo": payload,
                    "persistencia": persistencia,
                },
                ensure_ascii=False,
                indent=2,
            )
        )

    config = carregar_configuracoes()
    contexto_web = coletar_contexto_web(config)
    empresas_brutas = extrair_leads_brutos(config, contexto_web)

    empresas_com_cadastro = enriquecer_dados_empresa.partial(config=config).expand(
        empresa_raw=empresas_brutas
    )

    empresas_com_scraping = raspar_linkedin_decisores.partial(config=config).expand(
        empresa_raw=empresas_com_cadastro
    )

    empresas_com_emails = enriquecer_emails.partial(config=config).expand(
        empresa_raw=empresas_com_scraping
    )

    consolidado = consolidar_saida(empresas_com_emails)
    persistencia = persistir_saida_postgres(consolidado, config)
    imprimir_resumo(consolidado, persistencia)


dag_prospeccao_b2b()
