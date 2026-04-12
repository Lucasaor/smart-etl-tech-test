# Dicionário de Dados

## Visão Geral

Base de dados transacionais de uma operação de vendas de seguro automotivo via WhatsApp. Cada linha representa uma mensagem individual dentro de uma conversa.

**Números da base:**

- 15k conversas únicas
- Entre 120 a 150k mensagens
- Período: 01/02/2026 a 28/02/2026
- Formato: Parquet (.parquet)

## Distribuição das Conversas

| Tipo | % | Mensagens | Descrição |
|------|---|-----------|-----------|
| Lead frio / bounce | ~35% | 2-4 | Lead responde o primeiro contato mas some |
| Conversa curta | ~30% | 5-10 | Pergunta sobre preço ou cobertura, não avança |
| Conversa média | ~25% | 11-20 | Negociação completa com coleta de dados |
| Conversa longa | ~10% | 21-30+ | Vai e volta intenso, compara concorrentes |

## Imperfeições nos Dados

A base contém imperfeições propositais que simulam dados reais:

- **Nomes inconsistentes**: O campo `sender_name` varia entre maiúsculas, minúsculas, abreviações
- **Dados pessoais no texto livre**: CPF, CEP, e-mail, telefone e placa embutidos no `message_body`
- **Duplicidade de status**: Algumas mensagens com registros sent e delivered separados
- **Transcrições de áudio**: Mensagens do tipo audio possuem transcrição automática com erros
- **Mensagens vazias**: Stickers e imagens sem legenda resultam em `message_body` vazio
- **Dados do veículo não padronizados**: Marca, modelo, ano e placa em formatos variados
- **Menção a concorrentes**: Leads citam seguradoras concorrentes em linguagem informal

## Campos Variáveis

| Coluna | Tipo | Formato | Descrição |
|--------|------|---------|-----------|
| `message_id` | string | UUID hex 12 chars | Identificador único da mensagem |
| `conversation_id` | string | conv_XXXXXXXX | Agrupa todas as mensagens de uma conversa |
| `timestamp` | datetime | YYYY-MM-DD HH:MM:SS | Data e hora do envio |
| `sender_phone` | string | +55XXXXXXXXXXX | Telefone do remetente |
| `sender_name` | string | texto livre | Nome do remetente |
| `message_body` | string | texto livre | Conteúdo da mensagem |
| `campaign_id` | string | camp_XXX_fev2026 | Campanha de origem do lead |
| `agent_id` | string | agent_nome_NN | Vendedor humano responsável |
| `metadata` | string | JSON | Objeto JSON com dados contextuais |

## Campos Determinísticos

| Coluna | Tipo | Valores | Descrição |
|--------|------|---------|-----------|
| `direction` | string | outbound / inbound | Direção da mensagem |
| `message_type` | string | text / image / audio / document / sticker | Tipo da mensagem |
| `status` | string | sent / delivered / read / failed | Status de entrega |
| `channel` | string | whatsapp | Canal de comunicação (sempre whatsapp) |

## Notas Importantes

1. O vendedor sempre inicia a conversa
2. Um mesmo lead pode aparecer em mais de uma conversa (follow-ups)
3. `response_time_sec` é indicador direto de engajamento
4. Todos os dados são sintéticos (telefones, nomes, CPFs, e-mails e placas fictícios)
