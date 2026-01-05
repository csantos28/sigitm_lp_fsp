import logging
import warnings
import sys
from pathlib import Path

class SystemLogger:
    
    @staticmethod
    def configure_logger(name: str, log_file: str = 'sigitm_base_historica_lp_fsp.log') -> logging.Logger:
        """
        Configura um logger com handlers para arquivo e console.
        
        Args:
            name: Nome do logger (geralmente __name__)
            log_file: Nome do arquivo de log (padrão: 'system.log')
            
        Returns:
            Objeto logger configurado
        """
        # Ignora o aviso de estilos ausentes do openpyxl que polui o console
        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

        logger = logging.getLogger(name) # 1. Cria ou obtém um logger com o nome especificado
        logger.setLevel(logging.DEBUG) # 2. Define o nível mínimo de log (DEBUG captura tudo)

        # 3. Verifica se o logger já tem handlers para evitar duplicação
        if logger.handlers:
            return logger

        # 4. Cria o diretório de logs se não existir
        log_path = Path(log_file)
        if not log_path.parent.exists():
            log_path.parent.mkdir(parents=True)

        # 5. Configura o formato das mensagens de log:
        #    - Adiciona o nome do módulo onde ocorreu o log
        #    - Inclui o caminho completo do arquivo para exceções
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -  [%(filename)s:%(lineno)d] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # 6. Configura o handler para gravar em arquivo
        # Configura o handler para gravar em arquivo com UTF-8
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
        
        except TypeError:
            # Fallback para versões mais antigas do Python que não suportam encoding no FileHandler
            file_handler = logging.FileHandler(log_file)
        
        file_handler.setFormatter(formatter)

        # 7. Configura o handler para exibir no console com utf-8
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # 8. Adiciona ambos handlers ao logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger    