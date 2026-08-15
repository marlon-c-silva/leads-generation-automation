from __future__ import annotations

import logging
import re
from typing import Any

import requests
from pydantic import BaseModel, Field, TypeAdapter

logger = logging.getLogger(__name__)


def _gerar_conteudo_gemini(
    client: Any,
    *,
    model: str,
    contents: str,
    schema: dict[str, Any],
    temperature: float = 0.1,
    tools: list[Any] | None = None,
    types_module: Any | None = None,
) -> Any:
    """Gera conteúdo via Interactions API, priorizando o modelo gemini-2.5-flash e tentando aliases compatíveis em caso de 404."""
    from google.genai.errors import ClientError

    if types_module is None:
        from google.genai import types as types_module

    modelos = []
    for candidate in [model, "gemini-3.5-flash-lite", "gemini-3.5-flash", "gemini-2.5-flash", "gemini-2.5-flash-preview-05-20"]:
        if candidate and candidate not in modelos:
            modelos.append(candidate)

    ultimo_erro: Exception | None = None
    for modelo_atual in modelos:
        try:
            return client.models.generate_content(
                model=modelo_atual,
                contents=contents,
                config=types_module.GenerateContentConfig(
                    tools=tools,
                    response_mime_type="application/json",
                    response_schema=schema,
                    temperature=temperature,
                ),
            )
        except ClientError as exc:
            ultimo_erro = exc
            message = str(exc)
            if "404" not in message and "NOT_FOUND" not in message:
                raise
            logger.warning(
                "Modelo Gemini %s indisponível na Interactions API; tentando próximo alias. Detalhe: %s",
                modelo_atual,
                message,
            )

    if ultimo_erro is not None:
        raise ultimo_erro
    raise RuntimeError("Nenhum modelo Gemini disponível para geração de conteúdo.")


class Decisor(BaseModel):
    nome: str = Field(..., description="Nome do decisor")
    cargo: str | None = Field(default=None, description="Cargo do decisor")
    linkedin_url: str | None = Field(default=None, description="URL do perfil no LinkedIn")
    linkedin_id: str | None = Field(default=None, description="ID público do LinkedIn")
    email: str | None = Field(default=None, description="Email corporativo")


class Empresa(BaseModel):
    nome: str = Field(..., description="Nome da empresa")
    segmento: str | None = Field(default=None, description="Segmento da empresa")
    site: str | None = Field(default=None, description="Site institucional")
    linkedin_url: str | None = Field(default=None, description="URL da página da empresa")
    cnpj: str | None = Field(default=None, description="CNPJ da empresa")
    decisores: list[Decisor] = Field(default_factory=list, description="Lista de decisores")
    dados_cadastrais: dict[str, Any] | None = Field(
        default=None,
        description="Dados enriquecidos via minhareceita.org",
    )


class RespostaEmpresas(BaseModel):
    empresas: list[Empresa] = Field(default_factory=list)


def _normalizar_cnpj(cnpj: str | None) -> str | None:
    if not cnpj:
        return None
    apenas_digitos = re.sub(r"\D", "", cnpj)
    return apenas_digitos if len(apenas_digitos) == 14 else None


def _extrair_linkedin_id(linkedin_url: str | None) -> str | None:
    if not linkedin_url:
        return None

    cleaned = linkedin_url.strip().rstrip("/")
    if "/in/" in cleaned:
        return cleaned.split("/in/")[-1]
    if "/company/" in cleaned:
        return cleaned.split("/company/")[-1]
    return cleaned.split("/")[-1] if "/" in cleaned else cleaned


def extrair_empresas_e_decisores(
    prompt_busca: str,
    gemini_api_key: str,
    model: str = "gemini-2.5-flash",
    usar_google_search_tool: bool = True,
    contexto_web: str | None = None,
) -> list[Empresa]:
    """Extrai empresas e decisores com output estruturado, com ou sem Google Search tool."""

    try:
        from google import genai
        from google.genai import types
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Dependência 'google-genai' não encontrada. Instale com: pip install google-genai"
        ) from exc

    schema = {
        "type": "object",
        "properties": {
            "empresas": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "nome_empresa": {"type": "string"},
                        "site_oficial": {"type": "string"},
                        "linkedin_empresa_url": {"type": "string"},
                        "segmento": {"type": "string"},
                        "porte": {"type": "string"},
                        "cidade": {"type": "string"},
                        "estado": {"type": "string"},
                        "cnpj": {"type": "string"},
                        "decisores": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "nome": {"type": "string"},
                                    "cargo": {"type": "string"},
                                    "linkedin_url": {"type": "string"},
                                },
                                "required": ["nome"],
                            },
                        },
                    },
                    "required": ["nome_empresa", "decisores"],
                },
            }
        },
        "required": ["empresas"],
    }

    instruction_suffix = ""
    if contexto_web:
        instruction_suffix = (
            "\n\nCONTEXTO DE BUSCA EXTERNA (priorize estes achados e mantenha o schema):\n"
            f"{contexto_web}"
        )

    full_prompt = f"{prompt_busca}{instruction_suffix}"

    tools = [types.Tool(google_search=types.GoogleSearch())] if usar_google_search_tool else None

    client = genai.Client(api_key=gemini_api_key)
    response = _gerar_conteudo_gemini(
        client=client,
        model=model,
        contents=full_prompt,
        schema=schema,
        temperature=0.1,
        tools=tools,
        types_module=types,
    )

    raw_text = response.text or "[]"
    empresas: list[Empresa] = []

    try:
        resposta = RespostaEmpresas.model_validate_json(raw_text)
        empresas = resposta.empresas
    except Exception:
        try:
            # Compatibilidade com saída legada: lista direta de empresas.
            empresas_adapter = TypeAdapter(list[Empresa])
            empresas = empresas_adapter.validate_json(raw_text)
        except Exception:
            empresas = []

    empresas_normalizadas: list[Empresa] = []
    for item in empresas:
        empresas_normalizadas.append(item)

    # Compatibilidade com schema novo: nome_empresa/site_oficial/linkedin_empresa_url
    if not empresas_normalizadas:
        import json

        data = json.loads(raw_text)
        raw_empresas = data.get("empresas", []) if isinstance(data, dict) else []
        for raw_empresa in raw_empresas:
            raw_decisores = raw_empresa.get("decisores", [])
            decisores = [
                Decisor(
                    nome=raw_decisor.get("nome", ""),
                    cargo=raw_decisor.get("cargo"),
                    linkedin_url=raw_decisor.get("linkedin_url"),
                )
                for raw_decisor in raw_decisores
                if raw_decisor.get("nome")
            ]
            empresas_normalizadas.append(
                Empresa(
                    nome=raw_empresa.get("nome_empresa", ""),
                    segmento=raw_empresa.get("segmento"),
                    site=raw_empresa.get("site_oficial"),
                    linkedin_url=raw_empresa.get("linkedin_empresa_url"),
                    cnpj=raw_empresa.get("cnpj"),
                    decisores=decisores,
                )
            )

    empresas = [empresa for empresa in empresas_normalizadas if empresa.nome]

    for empresa in empresas:
        empresa.cnpj = _normalizar_cnpj(empresa.cnpj)
        for decisor in empresa.decisores:
            if not decisor.linkedin_id:
                decisor.linkedin_id = _extrair_linkedin_id(decisor.linkedin_url)

    return empresas


def buscar_dados_cnpj(
    cnpj: str,
    timeout_seconds: int = 20,
    brasil_api_base_url: str = "https://minhareceita.org",
) -> dict[str, Any] | None:
    """Consulta dados cadastrais de CNPJ em minhareceita.org."""

    cnpj_normalizado = _normalizar_cnpj(cnpj)
    if not cnpj_normalizado:
        return None

    base_url = (brasil_api_base_url or "https://minhareceita.org").rstrip("/")
    url = f"{base_url}/{cnpj_normalizado}"
    response = requests.get(url, timeout=timeout_seconds)

    if response.status_code == 404:
        logger.info("CNPJ %s não encontrado em %s", cnpj_normalizado, base_url)
        return None

    response.raise_for_status()
    return response.json()


def raspar_perfis_linkedin_apify(
    linkedin_urls: list[str],
    apify_api_token: str,
    actor_id: str = "LpVuK3Zozwuipa5bp",
) -> list[dict[str, Any]]:
    """Executa o actor da HarvestAPI no Apify para extrair dados de perfis do LinkedIn."""

    try:
        from apify_client import ApifyClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Dependência 'apify-client' não encontrada. Instale com: pip install apify-client"
        ) from exc

    if not linkedin_urls:
        return []

    client = ApifyClient(apify_api_token)
    run_input = {
        "startUrls": [{"url": url} for url in linkedin_urls],
    }

    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return []

    items = list(client.dataset(dataset_id).iterate_items())
    return items


def enriquecer_emails_kipflow(
    linkedin_ids: list[str],
    kipflow_api_key: str,
    timeout_seconds: int = 45,
    base_url: str = "https://api.kipflow.com",
) -> list[dict[str, Any]]:
    """Enriquece contatos na Kipflow para obter emails corporativos por linkedin_id."""

    ids_limpos = [item.strip() for item in linkedin_ids if item and item.strip()]
    if not ids_limpos:
        return []

    endpoint = f"{base_url.rstrip('/')}/contacts/v1/emails/batch"
    headers = {
        "Authorization": f"Bearer {kipflow_api_key}",
        "Content-Type": "application/json",
    }
    payload = {"linkedin_ids": ids_limpos}

    response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout_seconds)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "results", "contacts", "items"):
            maybe_list = data.get(key)
            if isinstance(maybe_list, list):
                return maybe_list
    return []


def buscar_resultados_web_serper(
    queries: list[str],
    serper_api_key: str,
    num_results: int = 5,
    timeout_seconds: int = 40,
) -> list[dict[str, Any]]:
    """Executa buscas web via Serper (Google Search API wrapper)."""

    if not serper_api_key or not queries:
        return []

    endpoint = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": serper_api_key,
        "Content-Type": "application/json",
    }

    all_results: list[dict[str, Any]] = []
    for query in queries:
        payload = {"q": query, "num": max(1, min(num_results, 10))}
        response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()

        organic = data.get("organic", []) if isinstance(data, dict) else []
        for item in organic:
            all_results.append(
                {
                    "query": query,
                    "title": item.get("title"),
                    "link": item.get("link"),
                    "snippet": item.get("snippet"),
                }
            )

    return all_results


def montar_contexto_web_para_prompt(
    resultados: list[dict[str, Any]],
    max_itens: int = 20,
) -> str:
    """Converte resultados de busca em contexto textual compacto para o prompt."""

    if not resultados:
        return ""

    linhas: list[str] = []
    for idx, item in enumerate(resultados[:max_itens], start=1):
        title = item.get("title") or "Sem titulo"
        link = item.get("link") or ""
        snippet = item.get("snippet") or ""
        query = item.get("query") or ""
        linhas.append(f"[{idx}] query={query} | title={title} | link={link} | snippet={snippet}")

    return "\n".join(linhas)
