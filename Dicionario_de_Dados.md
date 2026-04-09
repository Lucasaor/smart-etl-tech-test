## Visão Geral

Esta base contém dados transacionais de uma operação de vendas de seguro
automotivo via WhatsApp. Vendedores humanos prospectam e negociam
diretamente com leads.

Cada linha representa uma mensagem individual dentro de uma conversa.
Uma conversa é o conjunto de mensagens trocadas entre um vendedor e um
lead sobre uma cotação ou negociação de seguro.

A base reflete o comportamento real de uma operação de vendas:

**Números da base**

-   15k conversas únicas

-   Entre 120 a 150k mensagens

-   Período: 01/02/2026 a 28/02/2026

-   Formato: Parquet (.parquet)

**Lead frio / bounce (\~35%)**

-   De 2 a 4 mensagens.

-   Lead responde o primeiro contato mas some logo em seguida.

-   Vendedor tenta reengajar sem sucesso.

**Conversa curta (\~30%)**

-   De 5 a 10 mensagens.

-   Lead pergunta sobre preço ou cobertura, troca rápida, mas não avança
    > para cotação formal.

**Conversa média (\~25%)**

-   De 11 a 20 mensagens.

-   Negociação completa com coleta de dados do veículo, cotação, envio
    > de proposta, objeções sobre preço ou cobertura.

**Conversa longa (\~10%)**

-   De 21 a 30+ mensagens.

-   Vai e volta intenso.

-   Lead compara concorrentes, pede descontos, negocia condições.

-   Pode ou não fechar.

## 

##  

## Data Schema

A base contém imperfeições propositais que simulam dados reais.

**Nomes inconsistentes:** O campo sender_name dos leads varia entre
maiúsculas, minúsculas, abreviações, acentos e valores vazios. O mesmo
lead pode aparecer como \"Ana Paula Ribeiro\", \"ana paula\", \"Ana P.\"
em conversas diferentes.

**Dados pessoais no texto livre**: CPF, CEP, e-mail, telefone e placa
estão embutidos no message_body sem formato padronizado, misturados com
texto conversacional.

**Duplicidade de status**: Algumas mensagens aparecem com dois registros
(sent e delivered) representando eventos separados da plataforma.

**Transcrições de áudio**: Mensagens do tipo audio possuem transcrição
automática no body, que pode conter erros de reconhecimento de fala.

**Mensagens vazias**: Stickers e imagens sem legenda resultam em
message_body vazio ou nulo.

**Dados do veículo não padronizados**: Marca, modelo, ano e placa
aparecem misturados no texto em ordens e formatos variados (\"gol 2019
1.0 placa ABC1D23\", \"tenho um HB20 22\", \"Corolla 2021/2022 prata\").

**Menção a concorrentes**: Leads citam seguradoras concorrentes (Porto
Seguro, Azul, Bradesco Seguros, SulAmérica, Liberty, Allianz) e valores
pagos atualmente em linguagem informal.

**Histórico de sinistros**: Informações sobre sinistros anteriores
aparecem em linguagem natural, sem estrutura (\"tive 1 sinistro ano
passado, batida traseira num farol ali na paulista\").

### Notas Importantes

1.  O vendedor sempre inicia a conversa. A primeira mensagem de qualquer
    > conversation_id é outbound.

2.  Um mesmo lead pode aparecer em mais de uma conversa, representando
    > follow-ups ou recontatos em datas diferentes.

3.  O campo response_time_sec é um indicador direto de engajamento e
    > intenção de compra do lead.

4.  Todos os dados são sintéticos. Telefones, nomes, CPFs, e-mails e
    > placas são completamente fictícios.

### Colunas de campos variáveis

Campos cujo valor é livre e não pertence a um conjunto fixo de opções.

  ---------------------------------------------------------------------------------------------------------------------
  **Coluna**            **Type**    **Formato**        **Descrição**   **Exemplo**                     **Observação**
  --------------------- ----------- ------------------ --------------- ------------------------------- ----------------
  **message_id**        string      UUID hex 12 chars  Identificador   a1b2c3d4e5f6                    Nunca se repete
                                                       único da                                        
                                                       mensagem                                        

  **conversation_id**   string      conv_XXXXXXXX      Agrupa todas as conv_00012847                   Único por
                                                       mensagens de                                    conversa
                                                       uma conversa                                    

  **timestamp**         datetime    YYYY-MM-DD         Data e hora do  2026-02-05 09:12:43             
                                    HH:MM:SS           envio da                                        
                                                       mensagem                                        

  **sender_phone**      string      +55XXXXXXXXXXX     Telefone        +5511988734012                  Dados fictícios
                                                       completo do                                     
                                                       remetente                                       

  **sender_name**       string      texto livre        Nome do         Carlos Mendes                   Lead =
                                                       remetente                                       inconsistente
                                                                                                       (minúsc,
                                                                                                       abreviado,
                                                                                                       vazio); Vendedor
                                                                                                       = padronizado

  **message_body**      string      texto livre        Conteúdo da     gol 2019 1.0 placa ABC1D23      
                                                       mensagem com                                    
                                                       dados não                                       
                                                       estruturados                                    

  **campaign_id**       string      camp_XXX_fev2026   Campanha de     camp_landing_fev2026            8-10 campanhas;
                                                       origem do lead                                  mesmo valor em
                                                                                                       todas as msgs da
                                                                                                       conversa

  **agent_id**          string      agent_nome_NN      Vendedor humano agent_marcos_07                 15-20 vendedores
                                                       responsável                                     com distribuição
                                                                                                       desigual

  **metadata**          string      JSON               Objeto JSON com {\"device\":\"android\",\...}   Ver Tabela 3
                                                       dados                                           para detalhes
                                                       contextuais                                     dos campos
  ---------------------------------------------------------------------------------------------------------------------

# 

###  

### Colunas de campos determinísticos

Campos com valores pertencentes a um conjunto fixo e conhecido.

  --------------------------------------------------------------------------------------------------------------------
  **Coluna**                 **Type**    **Valor**             **Descrição**    **Exemplo**           **Observação**
  -------------------------- ----------- --------------------- ---------------- --------------------- ----------------
  **direction**              string      outbound              Mensagem enviada outbound              
                                                               pelo vendedor                          

                                         inbound               Mensagem enviada inbound               
                                                               pelo lead                              

  **message_type**           string      text                  Mensagem de      text                  
                                                               texto                                  

                                         image                 Imagem enviada   image                 

                                         audio                 Áudio com        audio                 Transcrição pode
                                                               transcrição                            conter erros
                                                               automática no                          
                                                               body                                   

                                         document              Documento anexo  document              
                                                               (PDF, proposta)                        

                                         sticker               Figurinha        sticker               

  **status**                 string      sent                  Mensagem enviada sent                  Pode haver
                                                                                                      duplicidade
                                                                                                      sent+delivered

                                         delivered             Entregue ao      delivered             
                                                               destinatário                           

                                         read                  Lida pelo        read                  
                                                               destinatário                           

                                         failed                Falha no envio   failed                

  **channel**                string      whatsapp              Canal de         whatsapp              Sempre whatsapp
                                                               comunicação                            nesta base

  **conversation_outcome**   string      venda_fechada         Lead assinou     venda_fechada         
                                                               proposta,                              
                                                               apólice emitida                        

                                         perdido_preco         Lead achou caro  perdido_preco         
                                                               e não fechou                           

                                         perdido_concorrente   Lead fechou com  perdido_concorrente   
                                                               outra seguradora                       

                                         ghosting              Lead parou de    ghosting              
                                                               responder                              

                                         desistencia_lead      Lead desistiu    desistencia_lead      
                                                               explicitamente                         

                                         proposta_enviada      Proposta         proposta_enviada      
                                                               enviada, sem                           
                                                               retorno                                

                                         em_negociacao         Conversa ainda   em_negociacao         
                                                               aberta                                 
  --------------------------------------------------------------------------------------------------------------------

# 

### Coluna com metadado

Campos dentro do objeto JSON da coluna metadata.

  ------------------------------------------------------------------------------------------------
  **Coluna**              **Type**    **Formato**   **Descrição**   **Exemplo**   **Observação**
  ----------------------- ----------- ------------- --------------- ------------- ----------------
  **device**              string      texto livre   Dispositivo do  android       
                                                    remetente                     

  **city**                string      texto livre   Cidade do       São Paulo     
                                                    remetente                     

  **state**               string      UF 2 chars    UF do remetente SP            

  **response_time_sec**   int \| null segundos      Tempo que o     187           
                                                    lead demorou                  
                                                    pra responder                 

  **is_business_hours**   boolean     true/false    Se foi enviada  true          
                                                    em horário                    
                                                    comercial                     
                                                    (08-18h                       
                                                    seg-sex)                      

  **lead_source**         string      texto livre   Origem do lead  google_ads    
                                                    antes da                      
                                                    campanha                      
  ------------------------------------------------------------------------------------------------
