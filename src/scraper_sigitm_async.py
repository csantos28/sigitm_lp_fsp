import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple
from playwright.async_api import async_playwright, Page, Playwright, Locator, BrowserContext
from platformdirs import user_downloads_dir
from pathlib import Path
from twocaptcha import TwoCaptcha
from .syslog import SystemLogger
from .psw import username, password, chave_api

class SIGITMAutomation:
    """
    Classe principal para automação do acesso ao sistema SIGITM da Vivo.
    
    Attributes:
        login_url (str): URL do sistema
        username (str): Login de rede do usuário
        password (str): Senha do usuário
        api_key_2captcha (str): Chave da API do 2captcha
        browser (Browser): Instância do browser
        context (BrowserContext): Contexto do browser
        page (Page): Página principal
        max_captcha_retries (int): Número máximo de tentativas para resolver captcha
    """    

    CONSULTA_NAME = "CONSULTA_TLP_PCP_LP_FSP_CS"

    def __init__(self):
        """
        Inicializa a classe de automação.
        
        Args:
            username (str): Login de rede do usuário
            password (str): Senha do usuário
            api_key_2captcha (str): Chave da API do 2captcha
        """ 

        self.login_url = "https://sigitm.vivo.com.br/app/app.jsp"
        self.username = username
        self.password = password
        self.api_key_2captcha = chave_api
        self.logger = SystemLogger.configure_logger('SIGITMAutomation')
        self.playwright_engine: Playwright = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.system_page: Page = None
        self.download_dir = Path(user_downloads_dir())
        self.max_captcha_retries = 5

    async def _setup_browser(self) -> Page:
        """
        Configuração do browser
        
        Returns:
            Page: Página configurada e pronta
        """

        # 🚀 Inicialização direta
        self.playwright_engine = await async_playwright().start()

        # Cria diretório para perfil persistente
        profile_path = Path("chrome_profile_normal")
        profile_path.mkdir(exist_ok=True)

        # ✅ CONTEXTO PERSISTENTE - todas as páginas herdam este perfil
        self.context = await self.playwright_engine.chromium.launch_persistent_context(
            user_data_dir=str(profile_path),
            headless=True,
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            accept_downloads=True,
            ignore_https_errors=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',           # Necessário em ambientes Linux/CI
                '--disable-gpu',          # Reduz o uso de recursos gráficos
                '--disable-dev-shm-usage',# Essencial para execução em Docker/CI
                '--no-default-browser-check' # Otimização de tempo de inicialização
                ]
        )

        # 🛡️ Script de indetectabilidade
        await self.context.add_init_script(
            """
            delete Object.getPrototypeOf(navigator).webdriver;
            window.chrome = { runtime: {} };
            """
        )

        # ✅ AMBAS AS OPÇÕES SÃO PERSISTENTES:
        # - context.pages[0] → página que já veio com o contexto persistente
        # - context.new_page() → nova página NO MESMO contexto persistente  
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()

        self.logger.info("✅ Browser configurado com sucesso")
        return self.page
    
    async def _load_page_coroutines(self, check_elements: list = None):
        """Corotinas para verificação de carregamento"""

        tasks = [
           self.page.wait_for_load_state('networkidle'), # 1️⃣ Rede ociosa
           self.page.wait_for_function("document.readyState === 'complete'") # 2️⃣ DOM completo
        ]

        # 3️⃣ Elementos específicos (opcional)
        if check_elements:
            for selector in check_elements:
                tasks.append(self.page.wait_for_selector(selector, state='visible', timeout=15000))

        # 🔄 Executa tudo em paralelo
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return all(not isinstance(result, Exception) for result in results)       

    async def _wait_for_page(self, step_name: str, timeout: int = 60, check_elements: list = None) -> bool:
        """
        🚀 Aguardar carregamento completo
        
        Args:
            step_name: Nome da etapa para logs
            timeout: Timeout total em segundos (não cumulativo)
            check_elements: Lista de seletores para verificar (opcional)
        """        

        self.logger.info(f"🌐 Aguardando carregamento: {step_name}")
        start_time = time.time()

        try:
            # ⚡ Estratégia em paralelo para melhor performance
            success = await asyncio.wait_for(self._load_page_coroutines(check_elements), timeout=timeout)

            load_time = time.time() - start_time

            if success:
                self.logger.info(f"✅ {step_name} carregado em {load_time:.1f}s")
                return True
            else:
                self.logger.error(f"❌ {step_name} - Alguns elementos não foram carregados")
                return False
        
        except asyncio.TimeoutError:
            self.logger.error(f"⌛ Timeout {timeout}s em: {step_name}")
            
            # Verifica se algum elemento crítico está presente mesmo com timeout
            if check_elements:
                for selector in check_elements:
                    try:
                        if await self.page.locator(selector).count() > 0:
                            self.logger.info(f"✅ Elemento {selector} encontrado mesmo com timeout")
                                
                            return True
                    except:
                        continue
            
            return False 
        
        except Exception as e:
            self.logger.error(f"❌ Erro em {step_name}: {e}")
            return False
    
    async def _locate_login_elements(self) -> Tuple[Locator, ...]:
        """
        Localiza e retorna os elementos necessários para o login.
        
        Returns:
            tuple: (username_field, password_field, captcha_image, captcha_field)
        """

        try:
            # 🎯 Cria todos os locators de uma vez
            username_field = self.page.locator("#username")
            password_field = self.page.locator("#password")
            captcha_image = self.page.locator('//*[@id="captcha"]')
            captcha_field = self.page.locator(".inp-capt")

            # ⚡ Aguarda TODOS em PARALELO
            await asyncio.gather(
                username_field.wait_for(state="visible", timeout=15000),
                password_field.wait_for(state="visible", timeout=15000),
                captcha_image.wait_for(state="visible", timeout=15000),
                captcha_field.wait_for(state="visible", timeout=15000)
            )

            self.logger.info("✅ Todos elementos de login localizados")

            return username_field, password_field, captcha_image, captcha_field
        
        except Exception as e:
            self.logger.error(f"❌ Falha ao localizar elementos: {e}")
            raise
    
    async def _solve_captcha(self, captcha_image: Locator) -> Optional[str]:
        """
        Resolve captcha
        
        Args:
            captcha_locator (Locator): Locator da imagem do captcha
            
        Returns:
            Optional[str]: Solução do captcha ou None
        """        

        self.logger.info("🔐 Resolvendo captcha...")

        try:
            # ✅ SALVAR EM ARQUIVO TEMPORÁRIO
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
                temp_path = tmp_file.name
            
            # Salvar screenshot no arquivo temporário
            await captcha_image.screenshot(path=temp_path)

            # ✅ SOLUÇÃO COM CAMINHO DO ARQUIVO
            solver = TwoCaptcha(self.api_key_2captcha)
            result = solver.normal(temp_path)

            # ✅ LIMPEZA AUTOMÁTICA - liberação imediata de memória
            try:
                os.unlink(temp_path)
                self.logger.debug("✅ Arquivo temporário removido")
            
            except:
                pass            

            if (solution := result.get('code')):
                self.logger.info(f"✅ Captcha resolvido: {solution}")
                return solution

        except Exception as e:
            self.logger.warning(f"❌ Erro inesperado ao processar CAPTCHA:: {str(e)[:100]}...")
            return None
    
    async def _fill_login_form(self) -> bool:
        """
        Preenche o formulário.
        
        Args:
            initial_captcha_src: Source inicial do captcha para comparação
            
        Returns:
            bool: True se bem-sucedido
        """          

        self.logger.info("🖊️ Preenchendo formulário...")

        # Buscar elementos
        elements = await self._locate_login_elements()
        username, password, captcha_image, captcha_field = elements

        if None in [username, password, captcha_image, captcha_field]:
            return False
        
        try:
            # Executar preenchimento em SEQUÊNCIA
            await username.fill(self.username)
            self.logger.info("✅ Usuário preenchido")

            await password.fill(self.password)
            self.logger.info("✅ Senha preenchida")

            captcha_solution = await self._solve_captcha(captcha_image)
            if not captcha_solution:
                return False
            
            await captcha_field.fill(captcha_solution)
            self.logger.info("✅ Captcha preenchido")

            await self.page.keyboard.press("Enter")
            self.logger.info("✅ Formulário submetido")

            return True

        except Exception as e:
            self.logger.error(f"❌ Erro no preenchimento: {e}")
            return False
    
    async def _wait_for_new_window(self, timeout: int = 30000) -> Optional[Page]:
        """
        Aguarda e retorna a nova janela aberta após o login
        
        Args:
            timeout: Timeout em milissegundos
            
        Returns:
            Optional[Page]: Nova página ou None
        """

        self.logger.info("🔄 Aguardando abertura de nova janela...")

        if not self.context:
            self.logger.error("❌ Contexto do browser não inicializado")
            return None
        
        start_time = time.time()
        old_page = self.page

        # Verificação de páginas existentes
        while (time.time() - start_time) * 1000 < timeout:
            try:
                pages = self.context.pages

                if len(pages) > 1:
                    for page in pages:
                        if page != self.page and not page.is_closed():
                            self.logger.info("✅ Nova janela encontrada")

                            # ✅ FECHA a página anterior
                            if not old_page.is_closed():
                                await old_page.close()
                                self.logger.info("🔚 Página anterior fechada")
                            
                            # ✅ Atualiza para a nova página
                            self.page = page
                            await self.page.bring_to_front()

                            return page
                
                # Pequena pausa entre verificações
                await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.warning(f"❌ Erro ao verificar páginas: {e}")
                break
            
        self.logger.warning(f"⌛ Timeout {timeout}ms - Nova janela não detectada")
        return None
    
    async def _verify_login_sucess(self, initial_captcha_src) -> bool:
        """
        Verifica sucesso do login de forma robusta com verificação de captcha
        
        Args:
            initial_captcha_src: Source inicial do captcha para detectar falhas
            
        Returns:
            bool: True se login bem-sucedido
        """         
        self.logger.info("🔍 Verificando sucesso do login...")

        # 🔍 1. PRIMEIRO: Verifica se o captcha mudou (indicando tentativa falha)
        if initial_captcha_src:         
            try:
                captcha_locator = self.page.locator('//*[@id="captcha"]')

                if await captcha_locator.is_visible(timeout=5000):
                    current_src = await captcha_locator.get_attribute("src")

                    if current_src != initial_captcha_src:
                        self.logger.warning("🔄 Captcha mudou - solução anterior estava incorreta")
                        return False
            
            except:
                #  Se não encontrou o captcha, provavelmente o login foi bem-sucedido
                pass

         # 🔍 2. SEGUNDO: Aguarda nova janela (indicador de sucesso)
        new_page = await self._wait_for_new_window()
        
        if new_page:
            try:
                # Aguarda o carregamento completo da nova página
                page_completed = await self._wait_for_page(step_name="Página Principal após Login", timeout=45, check_elements=["//*[contains(text(), 'Bem-vindo')]"])

                if page_completed:
                    self.logger.info("🎉 Login realizado com sucesso!")
                    return True
                
            except Exception as e:
                self.logger.error(f"❌ Erro durante verificação do login: {e}")

        return False

    async def _login(self) -> bool:
        """Executa o processo completo de login com verificação de captcha"""

        try:
            # Configuração inicial
            page = await self._setup_browser()
            await page.goto(self.login_url)
            await self._wait_for_page(step_name="Página de Login")

            # Tentativas de login
            for attempt in range(1, self.max_captcha_retries + 1):
                try:
                    self.logger.info(f"🔄 Tentativa de login {attempt}/{self.max_captcha_retries}")

                    # Obtém o source inicial do captcha para verificação posterior
                    elements = await self._locate_login_elements()
                    captcha_image = elements[2]
                    initial_captcha_src = await captcha_image.get_attribute("src")

                    # Preenche o formulário
                    if await self._fill_login_form():
                        # Aguarda um breve momento para processamento
                        await asyncio.sleep(1)

                        # Verifica se o login foi bem-sucedido (com verificação de captcha)
                        if await self._verify_login_sucess(initial_captcha_src):
                            return True
                        
                        else:
                            self.logger.warning(f"❌ Tentativa {attempt} falhou - captcha incorreto ou outro erro")
                            continue
                    
                    else:
                        self.logger.error(f"❌ Falha no preenchimento do formulário na tentativa {attempt}")
                        continue
                
                except Exception as e:
                    self.logger.error("❌ Todas as tentativas de login falharam")
                    return False
            
        except Exception as e:
            self.logger.error(f"❌ Erro crítico durante o login: {e}")
            return False
    
    async def _settings_consulta(self) -> bool:
        """
        Clica no menu 'Consultas' após o login bem-sucedido.
    
        Returns:
        bool: True se conseguiu clicar no menu, False caso contrário
        """

        self.logger.info("📋 Navegando para consulta específica...") 

        try:

            # 🔍 PASSO 1: Clicar no menu principal "Consulta"
            lista_consulta_locator = self.page.locator("span.x-panel-header-text", has_text="Consulta")

            if not await lista_consulta_locator.is_visible(timeout=15000):
                self.logger.error("❌ Menu 'Consulta' não está visível")
                return False
            
            await lista_consulta_locator.click()
            self.logger.info("✅ Menu 'Consulta' clicado.")

            # 🔍 PASSO 2: Clicar no item "Consultas"
            consultas_locator = self.page.locator("span.x-tree3-node-text", has_text="Consultas")

            if not await consultas_locator.is_visible(timeout=15000):
                self.logger.error("❌ Item 'Consultas' não está visível")
                return False
            
            await consultas_locator.click()
            self.logger.info("✅ Item 'Consultas' clicado.")

            # 🔍 PASSO 3: Aguardar carregamento da página de consultas
            element_xpath = f"//div[table//div[text()='{self.CONSULTA_NAME}']]"

            if not await self._wait_for_page(step_name="Página de Listagem de Consultas", check_elements=[element_xpath]):
                self.logger.error("❌ Timeout - Página de consultas não carregou")
                return False
            
            # 🔍 PASSO 4: Clicar na consulta específica
            consulta_locator = self.page.locator(element_xpath)

            if not await consultas_locator.is_visible(timeout=15000):
                self.logger.error(f"❌ Consulta '{self.CONSULTA_NAME}' não encontrada")
                return False

            await consulta_locator.dblclick()
            self.logger.info(f"✅ Consulta '{self.CONSULTA_NAME}' selecionada para edição.")

            await self._wait_for_page(step_name="Página de Edição da Consulta",
                                      check_elements=["button.x-btn-text:has-text('Executar')", "button.x-btn-text:has-text('Salvar')"])

            return True

        except Exception as e:
            self.logger.error(f"❌ Erro na navegação para consulta: {e}")
            return False

    async def _adjuste_date_and_execute_consulta(self) -> bool:

        btn_salvar_locator = self.page.locator("button.x-btn-text", has_text="Salvar")
        bnt_executar_locator = self.page.locator("button.x-btn-text", has_text="Executar")

        try:
            # 🔍 Localiza o campo específico de Data de Baixa
            field_data_de_baixa_locator = self.page.locator("xpath=//tr[.//span[text()='Data de Baixa']]//td[2]//b")

            if not await field_data_de_baixa_locator.is_visible(timeout=15000):
                self.logger.error("❌ Campo 'Data Encerramento' não encontrado")
                return False
            
            # 📅 Obtém a data atual antes da modificação
            data_ant = await field_data_de_baixa_locator.text_content()
                
            # 🖱️ Clica para habilitar a edição
            await field_data_de_baixa_locator.click()
            self.logger.info("✅ Campo 'Data de Baixa' clicado")            

            # ⏳ Aguarda um momento para o campo de edição aparecer
            await asyncio.sleep(0.5)

            # 🔍 Busca o campo de input ESPECÍFICO para data usando contexto mais preciso
            input_field_data = self.page.locator("input:focus")

            if not await input_field_data.is_visible(timeout=2000):
                self.logger.error("❌ Nenhum input adequado encontrado")   
                return False            

            self.logger.info("✅ Input localizado e em foco.")

            # 📝 Preenche a nova data
            current_date = (datetime.today() - timedelta(days=1)).strftime("%d/%m/%y")
            new_date = f"{current_date} 00:00"

            self.logger.info(f"🔄 Alterando data: {data_ant} → {new_date}")

            await input_field_data.click(force=True)
            await input_field_data.fill("")
            await input_field_data.fill(new_date)
            await self.page.keyboard.press("Enter")

            data_pos = await field_data_de_baixa_locator.text_content()

            if data_ant != data_pos or data_ant == new_date:
                self.logger.info("✅ Alteração de data confirmada.")

                await btn_salvar_locator.click()
                self.logger.info("💾 Alteração de data salva com sucesso!")
                    
                await asyncio.sleep(1)

                await bnt_executar_locator.click()
                self.logger.info("⚙️  Executando consulta...")

                await self._wait_for_page(step_name="Página de resultado da consulta", check_elements=["button.x-btn-text:has-text('Exportar')"])
                return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao ajustar data: {e}")
            return False
    
    async def _exportar_consulta(self) -> Optional[Path]:
            """
            Fluxo completo de exportação: clica em Exportar e aguarda download,
            usando o método nativo expect_download do Playwright.
            
            Returns:
                Path: Caminho do arquivo baixado ou None se falhou
            """
            try:
                self.logger.info("📤 Iniciando exportação da consulta...")       

                # 1. Localiza o botão Exportar
                btn_exportar = self.page.locator("button.x-btn-text", has_text="Exportar")
                if not await btn_exportar.is_visible(timeout=10000):
                    self.logger.error("❌ Botão 'Exportar' não encontrado")
                    return None
                
                # 2. ⚡ CRIA A EXPECTATIVA DE DOWNLOAD ANTES DA AÇÃO!
                #    Isso cria uma 'promessa' assíncrona que será resolvida quando o download começar
                async with self.page.expect_download(timeout=120000) as download_info:
                    
                    # 3. Executa a ação (o Playwright aguarda o clique completar e o download iniciar)
                    await btn_exportar.click()
                    self.logger.info("✅ Botão 'Exportar' clicado")

                # 4. Obtém o objeto Download (esta linha só é executada após o download começar)
                download = await download_info.value

                # 5. Salva o arquivo em um local definitivo e aguarda a conclusão do processo
                #    O save_as() aguarda a conclusão do download, que pode levar tempo.
                final_name = f"{download.suggested_filename}"
                final_path = self.download_dir / final_name
                
                await download.save_as(str(final_path))
                self.logger.info(f"💾 Download salvo em: {final_path}")

                # 6. Integração com a sua lógica de validação
                if await self._validate_downloaded_file(final_path):
                    self.logger.info("🎉 Exportação concluída e validada com sucesso!")
                    return final_path
                
                # 7. Fecha o download (libera memória)
                await download.delete()
                return None # Falhou na validação

            except Exception as e:
                self.logger.error(f"❌ Erro durante exportação: {e}")
                return None
    
    async def _wait_for_consulta_completion(self, timeout: int = 120) -> bool:

        self.logger.info("⏳ Aguardando conclusão da consulta...")
        start_time = time.time()

        async def check_completion():
            
            import re

            try:
                # 1. Verifica indicador de paginação
                indicator = self.page.locator("div.my-paging-display.x-component:has-text('A visualizar'):visible")

                if await indicator.count() == 0:
                    return False, "indicator_not_found"

                text = (await indicator.first.text_content()).strip()
                
                # 2. Verifica se tem formato válido e dados
                if "de" not in text or not any(c.isdigit() for c in text):
                    return False, "invalid_format"
                
                # 3. Extrai total de registros
                total_match = re.search(r'de\s+(\d+)', text)
                if not total_match:
                    return False, "no_total_found"
                
                total = int(total_match.group(1))
                return total > 0, f"complete_{text}"
            
            except Exception as e:
                return False, f"error_{str(e)[:50]}"
        
        # Estratégia de polling
        check_count = 0
        last_status = ""

        while (time.time() - start_time) < timeout:
            is_complete, status = await check_completion()
            check_count += 1

            if is_complete:
                elapsed = time.time() - start_time
                self.logger.info(f"🎉 Consulta concluída em {elapsed:.1f}s ({check_count} verificações)")
                return True
            
            if status != last_status:
                if ("complete" not in status and "error" not in status and status != "indicator_not_found"):
                    self.logger.debug(f"📊 Status: {status}")
                
                last_status = status
            
            # Intervalo adaptativo (mais frequente no início)
            await asyncio.sleep(1 if check_count < 10 else 2)
        
        self.logger.error(f"❌ Timeout após {timeout}s - Último status: {last_status}")
        return False
    
    async def _validate_downloaded_file(self, file_path: Path) -> bool:
        """
        Validação rápida do arquivo baixado.
        
        Args:
            file_path: Caminho do arquivo a validar
            
        Returns:
            bool: True se o arquivo é válido
        """

        try:
            # Verificação básica
            if not file_path.exists():
                return False

            file_size = file_path.stat().st_size

            if file_size == 0:
                self.logger.warning("❌ Arquivo vazio")
                return False    
            
            # Verificação rápida por extensão
            extension = file_path.suffix.lower()

            if extension in ('.xlsx', '.xls'):
                return await self._validate_excel(file_path)
            else:
                # Para outros tipos, apenas verifica se não está vazio
                self.logger.info(f"📄 Arquivo {extension} validado (tamanho: {file_size} bytes)")
                return True
        
        except Exception as e:
            self.logger.error(f"❌ Erro na validação: {e}")
    
    async def _validate_excel(self, file_path: Path) -> bool:
        """
        Validação rápida de Excel - verifica apenas se pode ser aberto.
        """        
        try:
            # Verificação leve - apenas tenta abrir o arquivo
            import openpyxl

            workbook = openpyxl.load_workbook(file_path, read_only=True)
            has_sheets = len(workbook.sheetnames) > 0
            workbook.close()

            if not has_sheets:
                self.logger.warning("❌ Excel sem planilhas")
                return False
            
            self.logger.info("✅ Excel validado com sucesso")
            return True
        
        except ImportError:
            self.logger.warning("⚠️ Openpyxl não disponível - validação de Excel ignorada")
            return True # Fallback
        except Exception as e:
            self.logger.error(f"❌ Excel corrompido ou inválido: {e}")
            return False

    async def execute_process_sigitm(self) -> Tuple[bool, Optional[Path]]:

        try:
            if await self._login():
                if await self._settings_consulta():
                    self.logger.info("📋 Editando o campo 'Data de Baixa' da consulta...")

                    if await self._adjuste_date_and_execute_consulta():
                        if await self._wait_for_consulta_completion():
                            
                            arquivo_exportado = await self._exportar_consulta()
                            if arquivo_exportado:
                                return True, arquivo_exportado
            
            return False, None
        except Exception as e:
            self.logger.error(f"❌ Falha no processo principal: {e}")
            return False, None

    async def close(self):
        """Fecha o browser e encerra o motor do Playwright garantindo a liberação de recursos"""

        try:
            if self.context:
                await self.context.close()
                self.logger.info("🔒 Contexto e Browser encerrados.")
            
            if hasattr(self, 'playwright_engine'):
                await self.playwright_engine.stop()
                self.logger.info("🔚 Motor Playwright finalizado.")
        
        except Exception as e:
            self.logger.error(f"⚠️ Erro ao fechar o browser: {e}")




if __name__ == '__main__':

    async def main():

        scraper = SIGITMAutomation()

        try:
            sucess = await scraper.execute_process_sigitm()

            if sucess:
                print("✅ Processo concluído com sucesso!")

                await asyncio.sleep(3)
            
            else:
                print("❌ Falha no processo")
        
        finally:
            await scraper.close()


    asyncio.run(main())