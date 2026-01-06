import asyncio
from pathlib import Path
from datetime import timedelta
import time
from typing import Optional, Tuple
import sys

# Importação dos módulos
from src.scraper_sigitm_async import SIGITMAutomation
from src.vpn_manager import VPNConnectionManager, VPNConfig
from src.process_data_sigitm import ExcelFileHandler
from src.connection_database import PostgreSQLHandler, PostgreSQLConfig
from src.psw import table_name
from src.syslog import SystemLogger

class MainOrchestrator:

    def __init__(self):
        self.logger = SystemLogger.configure_logger('ORQUESTRADOR')
        self.max_retries = 3
        self.retry_delay = 10
        self.vpn_config = VPNConfig()
        self.db_config = PostgreSQLConfig()

    async def _manage_vpn_connection(self) -> bool:
        """Gerencia a conexão VPN de forma não bloqueante (async wrapper)."""

        manager = VPNConnectionManager(self.vpn_config)

        try:
            status, message = await asyncio.wait_for(
                asyncio.to_thread(manager.connect_with_fallback),
                timeout=self.vpn_config.vpn_switch_timeout + 10 # Tempo máximo + margem
            )

            if status:
                self.logger.info(f"✅ Conexão VPN/Rede estabelecida: {message}")
                return True
            else:
                self.logger.error(f"❌ Falha na conexão VPN: {message}")
        except TimeoutError:
            self.logger.critical("❌ Timeout atingido durante a tentativa de conexão VPN.")
            return False
        except Exception as e:
            self.logger.critical(f"❌ Erro inesperado no gerenciamento de VPN: {e}")
            return False

    async def _extract_step(self) -> Tuple[bool, Optional[Path]]:
        """Executa a extração (Scraper) e garante o fechamento do browser."""

        scraper = SIGITMAutomation()

        try:
            self.logger.info("🎬 Iniciando extração no SIGITM...")
            return await scraper.execute_process_sigitm()
        finally:
            await scraper.close()
    
    def _load_step(self, file_path: Path) -> bool:
        """Executa Transformação (Pandas) e Carga (SQL)."""

        try:
            # 1. Transformação
            handler = ExcelFileHandler()

            # Passa como parâmetro o file_path recebido da extração, eliminando a redundância de procurar o arquivo novamente.
            result = handler.process_most_recent_file(file_path=file_path)

            if not result.success:
                self.logger.error(f"❌ Falha no tratamento: {result.message}")
                return False
            
            df = result.dataframe
            self.logger.info(f"✅ Dados tratados com sucesso. Linhas: {len(df)}")

            # 2. Carga
            with PostgreSQLHandler(self.db_config) as db:
                self.logger.info(f"✅  Conectado ao banco de dados: {self.db_config.dbname}")

                if not db.table_exists(table_name):
                    db.create_table_from_dataframe(df, table_name)
                    self.logger.info(f"📋 Tabela '{table_name}' criada com base no DataFrame.")
                
                db.bulk_insert_dataframe(df, table_name)
                self.logger.info(f"🎉 Carga de dados concluída com sucesso na tabela: {table_name}")
                
                handler.delete_most_recent_file(file_path=file_path)
                
                return True
        except Exception as e:
            self.logger.error(f"❌ Erro crítico na carga de dados: {e}")
            return False

    async def run_pipeline(self):
        """Método principal que orquestra o loop de retentativas."""

        start_time = time.perf_counter()
    
        for attempt in range(1, self.max_retries + 1):
            self.logger.info("🔥 INICIANDO PIPELINE 🔥")
            self.logger.info(f"🔁 TENTATIVA {attempt} de {self.max_retries}...")

            try:
                # Passo 0: VPN
                if not await self._manage_vpn_connection():
                    raise ConnectionError("❌ Falha de rede/VPN")
                
                # Passo 1: Extração
                success_ext, file_path = await self._extract_step()
                if not success_ext or not file_path:
                    raise RuntimeError("❌ Falha na extração dos dados")
                
                # Passo 2: Carga
                if self._load_step(file_path):
                    total_time = time.perf_counter() - start_time
                    readable_time = str(timedelta(seconds=int(total_time))).zfill(8)
                    self.logger.info(f"🔥 PIPELINE CONCLUÍDO EM {readable_time}s! 🔥")
                    return sys.exit(0)
            
            except Exception as e:
                self.logger.warning(f"⚠️ Falha na tentativa {attempt}: {e}")
                if attempt <= self.max_retries:
                    self.logger.info(f"⏳ Aguardando {self.retry_delay}s para reiniciar...")
                    await asyncio.sleep(self.retry_delay)
        
        self.logger.critical("🛑 Falha definitiva após todas as tentativas.")
        sys.exit(1)


if __name__ == "__main__":
    # Inicia o loop de eventos assíncronos do Python
    orchestrator = MainOrchestrator()
    asyncio.run(orchestrator.run_pipeline())