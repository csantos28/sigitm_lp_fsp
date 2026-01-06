import subprocess
import os
import time
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
import pywinauto
from pywinauto.application import WindowSpecification
from pywinauto.controls.uia_controls import ListItemWrapper
from .syslog import SystemLogger
from .psw import vpn_rj_name, vpn_rj_gateway, vpn_bh_name, vpn_bh_gateway, corporate_gateway, ssl_gateway

@dataclass
class VPNConfig:
    """
    Configurações para gerenciamento de conexões VPN corporativas.
    
    Attributes:
        vpn_rj_name (str): Nome da VPN do Rio de Janeiro como aparece nas configurações do Windows
        vpn_rj_gateway (str): Endereço IP do gateway da VPN-RJ (ex: '189.112.73.237')
        vpn_bh_name (str): Nome da VPN de Belo Horizonte
        vpn_bh_gateway (str): Endereço IP do gateway da VPN-BH (ex: '186.248.137.162')
        corporate_gateway (str): IP do gateway quando conectado diretamente na rede corporativa
        max_retries (int): Número máximo de tentativas de conexão (padrão: 3)
        retry_delay (int): Tempo de espera entre tentativas em segundos (padrão: 5)
        connection_timeout (int): Tempo máximo para estabelecer conexão (padrão: 30s)
        stability_check_delay (int): Tempo para verificar estabilidade (padrão: 3s)
        vpn_switch_timeout (int): Tempo máximo para trocar entre VPNs (padrão: 15s)
        ui_load_timeout (int): Tempo máximo para carregar elementos da UI (padrão: 10s)
    """
    
    vpn_rj_name: str = vpn_rj_name
    vpn_rj_gateway: str = vpn_rj_gateway
    vpn_bh_name: str = vpn_bh_name
    vpn_bh_gateway: str = vpn_bh_gateway
    corporate_gateway: str = corporate_gateway
    ssl_gateway: str = ssl_gateway
    max_retries: int = 3
    retry_delay: int = 5
    connection_timeout: int = 30
    stability_check_delay: int = 3
    vpn_switch_timeout: int = 30
    ui_load_timeout: int = 10  # Tempo máximo para carregar elementos da UI

    def __post_init__(self):
        """Valida automaticamente as configurações ao inicializar."""
        self._validate_config()

    def _validate_config(self):
        """
        Valida todos os parâmetros de configuração.
        
        Raises:
            ValueError: Se qualquer parâmetro estiver inválido
        """
        ip_fields = [
            self.vpn_rj_gateway, 
            self.vpn_bh_gateway, 
            self.corporate_gateway,
            self.ssl_gateway
        ]
        
        if not all(isinstance(attr, str) for attr in [self.vpn_rj_name, self.vpn_bh_name] + ip_fields):
            raise ValueError("⚠️ Todos os parâmetros da VPN devem ser strings")
        
        for ip in ip_fields:
            if not self._is_valid_ip(ip):
                raise ValueError(f"❌ IP inválido: {ip}")

    @staticmethod
    def _is_valid_ip(ip: str) -> bool:
        """
        Valida se um string é um endereço IPv4 válido.
        
        Args:
            ip: String contendo o endereço IP
            
        Returns:
            bool: True se for um IPv4 válido, False caso contrário
        """
        try:
            segments = ip.split('.')
            if len(segments) != 4:
                return False
            return all(0 <= int(seg) <= 255 and seg == str(int(seg)) for seg in segments)
        except ValueError:
            return False

class VPNConnectionManager:
    """
    Gerenciador robusto de conexões VPN corporativas com fallback automático.
    
    Implementa um sistema de tentativas hierárquicas:
    1. Verifica conexão direta com a rede corporativa
    2. Tenta conectar na VPN-RJ (prioritária)
    3. Fallback para VPN-BH em caso de falha
    
    Features:
    - Detecção automática do estado atual da conexão
    - Múltiplas estratégias para interagir com a interface gráfica
    - Logs detalhados para troubleshooting
    - Cache de estado para melhor performance
    """
    
    def __init__(self, config: VPNConfig):
        """
        Inicializa o gerenciador com as configurações fornecidas.
        
        Args:
            config: Objeto VPNConfig com todas as configurações necessárias
        """        
        self.config = config
        self._os_type = 'nt' if os.name == 'nt' else 'posix'
        self._route_command = 'route print' if self._os_type == 'nt' else 'ip route'
        self._vpn_settings_command = ["start", "ms-settings:network-vpn"]
        self.logger = SystemLogger.configure_logger("VPNManager")
        self._last_status_check = 0
        self._status_cache = None
        self._cache_timeout = 5
        self._current_vpn = None

    def _get_active_gateway(self, force_check: bool = False) -> Optional[str]:
        """
        Identifica qual gateway está ativo na tabela de roteamento.
        
        Args:
            force_check: Se True, ignora o cache e força nova verificação
            
        Returns:
            str: 'corporate', 'ssl', 'rj', 'bh' indicando o gateway ativo ou None se nenhum
        """
        if not force_check and time.time() - self._last_status_check < self._cache_timeout:
            return self._status_cache
            
        try:
            result = subprocess.run(
                self._route_command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                timeout=5
            )
            
            output = result.stdout
            if self.config.corporate_gateway in output:
                self._status_cache = 'corporate'
            elif self.config.ssl_gateway in output:
                self._status_cache = 'ssl'
            elif self.config.vpn_rj_gateway in output:
                self._status_cache = 'rj'
            elif self.config.vpn_bh_gateway in output:
                self._status_cache = 'bh'
            else:
                self._status_cache = None
                
            self._last_status_check = time.time()
            return self._status_cache
            
        except subprocess.SubprocessError as e:
            self.logger.warning(f"❌ Erro ao verificar gateway: {e.stderr or str(e)}")
            return None

    def connect_with_fallback(self) -> Tuple[bool, str]:
        """
        Executa o fluxo completo de conexão com fallback automático.
        
        Returns:
            Tuple[bool, str]: 
                - Status (True = conectado, False = falha)
                - Mensagem detalhada do resultado
        """
       # 1. Verificação inicial
        self.logger.info("🔍 Verificando se há alguma VPN contecatada...")
        current_gateway = self._get_active_gateway(force_check=True)
        self._update_current_vpn(current_gateway)
        
        if current_gateway == 'corporate':
            return True, "Conectado diretamente à rede corporativa - sem necessidade de usar VPN. "
        if current_gateway == 'ssl':
            return True, 'Já conectado à SSL VPN-Plus'
        if current_gateway == 'rj':
            return True, "Já conectado à VPN-RJ"
        if current_gateway == 'bh':
            return True, "Já conectado à VPN-BH"

        self.logger.info("❌ Nenhuma VPN conectada.")

        # 2. Tentativa na VPN-BH
        self.logger.info("🔃 Iniciando conexão com VPN-BH...")
        status, message = self._connect_to_vpn(
            vpn_name=self.config.vpn_bh_name,
            expected_gateway='bh'
        )
        
        if status:
            return True, message

        # 3. Fallback para VPN-RJ
        self.logger.info("ℹ️ Ativando fallback para VPN-RJ...")
        return self._connect_to_vpn(
            vpn_name=self.config.vpn_rj_name,
            expected_gateway='rj'
        )

    def _connect_to_vpn(self, vpn_name: str, expected_gateway: str) -> Tuple[bool, str]:
        """
        Tenta conectar a uma VPN específica com verificação.
        
        Args:
            vpn_name: Nome da VPN como aparece nas configurações
            expected_gateway: Gateway esperado ('rj' ou 'bh')
            
        Returns:
            Tuple[bool, str]: Status e mensagem de resultado
        """
                
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Verificação rápida antes de tentar
                if self._get_active_gateway(force_check=True) == expected_gateway:
                    self._update_current_vpn(expected_gateway)
                    return True, f"Já conectado à {vpn_name}"

                # Tentativa de conexão
                if self._attempt_vpn_connection(vpn_name):
                    # Verificação da conexão
                    if self._verify_vpn_connection(expected_gateway):
                        return True, f"Conexão estabelecida com {vpn_name}"
                
                # Pausa entre tentativas
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay)
                    
            except Exception as e:
                self.logger.error(f"❌ Tentativa {attempt} falhou: {str(e)}")
                time.sleep(self.config.retry_delay)
        
        return False, f"Falha ao conectar à {vpn_name} após {self.config.max_retries} tentativas"

    def _attempt_vpn_connection(self, vpn_name: str) -> bool:
        """
        Realiza uma tentativa completa de conexão via interface gráfica.
        
        Args:
            vpn_name: Nome da VPN a ser conectada
            
        Returns:
            bool: True se a tentativa foi bem-sucedida
        """
        try:
            self.logger.debug("Abrindo janela de configurações...")
            window = self._open_vpn_settings_window()
            if not window:
                self.logger.error("Falha ao abrir janela de configurações")
                return False

            # Localiza a VPN na lista
            self.logger.debug("🔍 Localizando VPN na lista...")
            vpn_item = self._find_vpn_in_list(window, vpn_name)
            if not vpn_item:
                window.close()
                return False

            # Tenta conectar
            if not self._click_connect_button(window, vpn_name):
                window.close()
                return False

            window.close()
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro durante conexão: {str(e)}")
            return False

    def _find_vpn_in_list(self, window: WindowSpecification, vpn_name: str) -> Optional[ListItemWrapper]:
        """
        Localiza a VPN na lista de conexões usando múltiplas estratégias.
        
        Estratégias tentadas:
        1. Busca direta pelo ID automático da lista
        2. Busca iterativa por todos os itens da lista
        
        Args:
            window: Janela de configurações
            vpn_name: Nome da VPN a ser encontrada
            
        Returns:
            ListItemWrapper: Item da VPN encontrado ou None
        """
        try:

            # Tentativa 1: Busca direta
            try:
                vpn_list = window.child_window(
                    auto_id="SystemSettings_Vpn_Connections_ListView",
                    control_type="List",
                    timeout=self.config.ui_load_timeout
                )
                item = vpn_list.child_window(title=vpn_name, control_type="ListItem", timeout=3)
                # self.logger.debug("VPN encontrada via busca direta (auto_id)")
                return item
            except:
                pass

            # Tentativa 2: Busca iterativa
            try:
                vpn_list = window.child_window(
                    control_type="List",
                    timeout=self.config.ui_load_timeout
                )
                for idx, item in enumerate(vpn_list.children()):
                    if vpn_name.lower() in item.window_text().lower():
                        self.logger.debug(f"✅ VPN encontrada via busca iterativa (item {idx + 1})")
                        return item
            except Exception as e:
                self.logger.debug(f"❌ Busca iterativa falhou: {str(e)}")
                self.logger.warning(f"❌ VPN '{vpn_name}' não encontrada na lista")
                return None

        except Exception as e:
            self.logger.error(f"Erro ao localizar VPN: {str(e)}")
            return None

    def _click_connect_button(self, window: WindowSpecification, vpn_name: str) -> bool:
        """
        Tenta clicar no botão 'Conectar' usando apenas:
        1. Botão Conectar específico
        2. Busca global
        
        Args:
            window: Janela de configurações
            vpn_name: Nome da VPN para contexto
            
        Returns:
            bool: True se o clique foi bem-sucedido
        """
        self.logger.debug(f"⌛ Tentando conectar a VPN: {vpn_name}")

        try:
            # Primeiro localiza o item correto da VPN
            vpn_item = self._find_vpn_in_list(window, vpn_name)
            if not vpn_item:
                self.logger.error(f"❌ Item da VPN {vpn_name} não encontrado")
                return False

            # Abordagem 1: Botão Conectar específico
            try:
                # Clica no item primeiro para garantir foco
                vpn_item.click_input()
                time.sleep(2)

                # Tenta encontrar o botão Conectar
                connect_button = self._find_connect_button(window)
                if connect_button:
                    connect_button.click_input()
                    time.sleep(3)

                    if self._verify_connection_success(window, vpn_name):
                        self.logger.info(f"✅ Conexão estabelecida com {vpn_name} (via botão específico)")
                        return True
            except Exception:
                pass # Erro esperado, não registra

            # Abordagem 2: Busca global
            try:
                # Encontra todos os botões "Conectar"
                connect_buttons = [
                    btn for btn in window.descendants(control_type="Button") if "Conectar" in btn.window_text()
                ]
                
                if connect_buttons:
                    # Tenta encontrar o botão associado à VPN correta
                    for btn in connect_buttons:
                        try:
                            if vpn_name in btn.parent().window_text():
                                btn.click_input()
                                time.sleep(3)

                                if self._verify_connection_success(window, vpn_name):
                                    self.logger.info(f"✅ Conexão estabelecida com {vpn_name} (via busca global)")
                                    return True
                        except:
                            continue
                    
                    # Fallback: clica no primeiro botão Conectar
                    connect_buttons[0].click_input()
                    time.sleep(3)

                    if self._verify_connection_success(window, vpn_name):
                        self.logger.info(f"✅ Conexão estabelecida com {vpn_name} (via fallback global)")
                        return True
                    else:
                        # Verifica se conectou em outra VPN por engano
                        active_vpn = self._get_active_vpn_name(window)
                        if active_vpn and active_vpn != vpn_name:
                            self._disconnect_vpn(window, active_vpn)
            except Exception as e:
                self.logger.debug(f"❌ Erro durante busca global: {str(e)}")

            self.logger.error(f"❌ Falha ao conectar à {vpn_name} após todas as tentativas")
            return False

        except Exception as e:
            self.logger.error(f"❌ Erro crítico durante conexão: {str(e)}", exc_info=True)
            return False
        
    def _find_connect_button(self, window: WindowSpecification) -> Optional[WindowSpecification]:
        """Tenta encontrar o botão Conectar usando múltiplas estratégias."""
        try:
            return window.child_window(title="Conectar", control_type="Button", found_index=0)
        except:
            try:
                return window.child_window(auto_id="ConnectButton", control_type="Button")
            except:
                return None
            
    def _verify_connection_success(self, window: WindowSpecification, vpn_name: str) -> bool:
        """Verifica se a conexão foi realmente estabelecida."""
        try:
            # Verifica na UI se a VPN está conectada
            active_vpn = self._get_active_vpn_name(window)
            if active_vpn == vpn_name:
                return True
            
            # Verificação adicional na tabela de roteamento
            time.sleep(2)  # Espera para a conexão estabilizar
            gateway = self._get_active_gateway(force_check=True)
            
            if (vpn_name == self.config.vpn_rj_name and gateway == 'rj') or \
            (vpn_name == self.config.vpn_bh_name and gateway == 'bh'):
                return True
                
            return False
        except:
            return False

    def _get_active_vpn_name(self, window: WindowSpecification) -> Optional[bool]:
        """Obtém o nome da VPN que está atualmente conectada."""
        try:
            connected_items = window.descendants(control_type="ListItem")
            for item in connected_items:
                try:
                    if "Desconectar" in item.window_text():
                        return item.window_text().replace("Desconectar", "").strip()
                except:
                    continue
            return None
        except Exception as e:
            self.logger.debug(f"Erro ao verificar VPN ativa: {str(e)}")
            return None

    def _disconnect_vpn(self, window: WindowSpecification, vpn_name: str) -> bool:
        """Desconecta uma VPN específica."""
        try:
            vpn_item = self._find_vpn_in_list(window, vpn_name)
            if not vpn_item:
                return False
            
            disconnect_button = vpn_item.parent().child_window(
                title="Desconectar",
                control_type="Button",
                timeout=5
            )
            disconnect_button.click_input()
            time.sleep(3)
            self.logger.info(f"ℹ️ VPN {vpn_name} desconectada")
            return True
        except Exception as e:
            self.logger.error(f"❌ Falha ao desconectar {vpn_name}: {str(e)}")
            return False

    def _verify_vpn_connection(self, expected_gateway: str) -> bool:
        """
        Verifica se a VPN está realmente conectada após a tentativa.
        
        Args:
            expected_gateway: Gateway esperado ('rj' ou 'bh')
            
        Returns:
            bool: True se a conexão foi verificada
        """
        start_time = time.time()
        while time.time() - start_time < self.config.vpn_switch_timeout:
            if self._get_active_gateway(force_check=True) == expected_gateway:
                self._update_current_vpn(expected_gateway)
                return True
            time.sleep(3)
        
        self.logger.warning("❌ Falha ao verificar conexão dentro do timeout")
        return False

    def _update_current_vpn(self, gateway: Optional[str]) -> None:
        """
        Atualiza o estado interno da VPN atual.
        
        Args:
            gateway: 'corporate', 'rj', 'bh' ou None
        """
        if gateway == 'rj':
            self._current_vpn = self.config.vpn_rj_name
        elif gateway == 'bh':
            self._current_vpn = self.config.vpn_bh_name
        else:
            self._current_vpn = None

    def _open_vpn_settings_window(self) -> Optional[WindowSpecification]:
        """
        Abre a janela de configurações de VPN do Windows.
        
        Returns:
            WindowSpecification: Objeto da janela ou None se falhar
        """
        try:
            subprocess.run(
                self._vpn_settings_command,
                shell=True,
                check=True,
                timeout=10
            )
            time.sleep(3)  # Tempo para a janela carregar
            
            # Tenta encontrar a janela com vários identificadores
            desktop = pywinauto.Desktop(backend="uia")
            try:
                window = desktop["Configurações de VPN"]
                return window
            except:
                window = desktop["VPN settings"]
                return window
            
        except Exception as e:
            self.logger.error(f"Falha ao abrir interface VPN: {str(e)}")
            return None

    def get_connection_status(self) -> Dict:
        """
        Obtém o status detalhado atual da conexão.
        
        Returns:
            dict: {
                'status': bool,
                'connection_type': 'corporate'|'rj'|'bh'|None,
                'current_vpn': str|None,
                'last_check': str (timestamp)
            }
        """
        gateway = self._get_active_gateway(force_check=True)
        self._update_current_vpn(gateway)
        
        return {
            'status': gateway is not None,
            'connection_type': gateway,
            'current_vpn': self._current_vpn,
            'last_check': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_status_check))
        }