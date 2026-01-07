# Pipeline de Automação SIGITM - Base Histórica (LP_FSP)


## 🎯 Objetivo

Esta pipeline automatiza a extração, transformação e carga (ETL) de dados históricos de bilhetes fechados do sistema SIGITM da Vivo, armazenando-os em um banco de dados PostgreSQL para posterior análise e geração de insights.

O processo é executado de forma robusta e resiliente, com tratamento de erros, reconexão automática via VPN, validação de dados e múltiplas tentativas em caso de falhas.

## 🏗️ Arquitetura da Pipeline

```text
[VPN Manager] -> [Web Scraper] -> [Processador Excel] -> [Banco PostgreSQL]
        ↑               ↑                    ↑                    ↑
  Conexão de      Extração de          Transformação        Armazenamento
  Rede/VPN        Dados Web             dos Dados              em BD

```

## 📦 Módulos e Funcionalidades


### 1. `main_base_sigitm_lp_fsp.py` - **Orquestrador Principal**

**Papel**: Controla o fluxo completo da pipeline, coordenando a execução sequencial dos módulos.

**Funcionalidades**:

- Gerencia retentativas automáticas (até 3 tentativas | configurável)
- Orquestra a ordem de execução: VPN → Scraping → Processamento → Carga
- Calcula e reporta tempo total de execução
- Encerra o processo com códigos de saída apropriados (0=sucesso, 1=falha)


### 2. `vpn_manager.py` - **Gerenciador de Conexões**

**Papel**: Garante conectividade com a rede corporativa antes de iniciar o scraping.

**Funcionalidades**:

- Detecta automaticamente o estado atual da conexão
- Tenta conexão hierárquica:
  1. Rede corporativa direta (gateway interno)
  2. VPN-BH (Belo Horizonte)
  3. VPN-RJ (Rio de Janeiro) como fallback
- Interage com a interface gráfica do Windows 11 via `pywinauto`
- Implementa cache de status para performance
- Logs detalhados para troubleshooting

#### Configurações suportadas:

- VPN nativa do Windows 11
- SSL VPN-Plus corporativa


### 3. `scraper_sigitm_async.py` - **Extrator Web**

**Papel**: Acessa o sistema SIGITM via browser automatizado e extrai os dados.

**Funcionalidades**:

- **Autenticação** automatizada com:
  - Preenchimento de credenciais
  - Resolução de CAPTCHA via API 2Captcha
  - Tratamento de múltiplas tentativas (até 5)
- Navegação até a consulta específica "CONSULTA_TLP_PCP_LP_FSP_CS"
- Ajuste automático de datas (sempre busca dados em D-1 | configurável)
- Execução da consulta com monitoramento de conclusão
- Exportação da base em Excel usando o método nativo `expect_download` do Playwright
- Validação do arquivo baixado (tamanho, integridade, formato)

#### Tecnologias:

- Playwright (Chromium) em modo headless
- Contexto persistente para performance
- Scripts de anti-detecção


### 4. `process_data_sigitm.py` - **Processador de Dados**

**Papel**: Transforma o Excel bruto em dados estruturados para o banco.

**Funcionalidades**:

- Carregamento inteligente do arquivo mais recente
- Mapeamento de colunas para nomes padronizados (snake_case)
- Tratamento de datas:
  - Conversão para formato brasileiro (DD/MM/YYYY)
  - Filtro por data de corte (encerrados até ontem 23:59:59)
  - Normalização para fuso horário BRT
- Limpeza de dados:
  - Tratamento de valores nulos (NaN, NaT, "None", "")
  - Normalização de IDs (inteiros seguros)
  - Remoção de colunas desnecessárias (VTA PK)
- Validação de tipos e consistência


### 5. `connection_database.py` - **Gerenciador de Banco de Dados**

**Papel**: Gerencia toda a interação com o PostgreSQL.

**Funcionalidades**:

- Conexão segura com tratamento de erros e reconexão automática
- Mapeamento automático de tipos pandas → PostgreSQL
- Criação dinâmica de tabelas baseada na estrutura do DataFrame
- Inserção em massa otimizada:
  - `execute_batch()` para volumes médios
  - `COPY` protocolo para grandes volumes (mais eficiente)
- Operações DDL (CREATE, TRUNCATE, ALTER)
- Consultas parametrizadas com retorno tipado
- Context manager para gerenciamento automático de recursos

#### Recursos avançados:

- Configuração de schema e search_path
- Timezone UTC-3 configurado
- Logs detalhados de todas as operações


### 6. `syslog.py` - **Sistema de Logs**

**Papel**: Centraliza e padroniza o logging em toda a aplicação.

**Funcionalidades**:

- Logs simultâneos para arquivo e console
- Formato padronizado com timestamp, módulo, nível e localização
- Suporte a UTF-8 para caracteres especiais
- Filtro de warnings irrelevantes (ex: openpyxl)
- Rotação automática (apenas um arquivo)


## 🔄 Fluxo de Execução

1. Inicialização
   - Configuração de logs
   - Carregamento de credenciais (arquivo psw.py)

2. Fase 1 - Conectividade
   - Verificação do gateway ativo
   - Conexão VPN se necessário (com fallback)

3.  Fase 2 - Extração
    - Login no SIGITM (com CAPTCHA)
    - Navegação até consulta específica
    - Ajuste de data de encerramento
    - Execução da consulta
    - Exportação para Excel

4. Fase 3 - Transformação
    - Carregamento do Excel
    - Mapeamento e limpeza de colunas
    - Tratamento de datas e valores
    - Filtro por data de corte

5. Fase 4 - Carga
    - Conexão ao PostgreSQL
    - Criação da tabela (se não existir)
    - Inserção em massa dos dados
    - Exclusão do arquivo temporário

6. Finalização
    - Log de sucesso com tempo total
    - Encerramento limpo de recursos


## 🛡️ Recursos de Resiliência

- Retentativas automáticas (até 3x com delay exponencial | configurável)
- Fallback de VPN (RJ ← BH)
- Verificação de integridade em cada etapa
- Rollback automático em falhas de banco
- Timeout configurável para todas as operações
- Logs detalhados para diagnóstico


##  🚀 Como Executar

```bash
python main_base_sigitm_lp_fsp.py

```

### Pré-requisitos:

- Windows 11 (para VPN nativa)
- Python 3.9+
- PostgreSQL 12+
- Credenciais SIGITM e 2Captcha configuradas
- Conexão de rede corporativa disponível


## 📈 Valor Gerado

Esta pipeline transforma um processo manual e propenso a erros em um fluxo automático, confiável e auditável, permitindo:

1. Atualização diária automática da base histórica
2. Padronização dos dados para análise
3. Redução de esforço manual em ~90%
4. Melhoria na qualidade dos dados (validações automatizadas)


## 🔧 Manutenção

- **Configurações**: Centralizadas em psw.py (credenciais) e dataclasses
- **Logs**: Arquivo único com rotação manual
- **Monitoramento**: Via logs e códigos de saída
- **Escalabilidade**: Projetado para aumentar volume sem reestruturação