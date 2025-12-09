import sys
import time
import threading
from contextlib import contextmanager

@contextmanager
def loading_animation(initial_message: str = "Carregando..."):
    """
    Gerenciador de contexto que exibe uma animação e 'yields'
    o dicionário de status para permitir atualizações da mensagem.
    """
    # --- MUDANÇA: Cria um dicionário para compartilhar o status ---
    status_data = {'message': initial_message}
    stop_event = threading.Event()
    
    animation_thread = threading.Thread(
        target=_animate_loading,
        # --- MUDANÇA: Passa o dicionário para a thread ---
        args=(stop_event, status_data)
    )
    animation_thread.start()
    
    try:
        # --- MUDANÇA: 'Entrega' o dicionário para o bloco 'with' ---
        yield status_data
    finally:
        # Garante que a thread pare
        stop_event.set()
        animation_thread.join()
        
        

def _animate_loading(stop_event: threading.Event, status_data: dict):
    """
    (Função auxiliar) Exibe animação lendo a mensagem de um dict.
    """
    chars = ["   ", ".  ", ".. ", "..."]
    idx = 0
    last_line_len = 0 # Para limpar a linha corretamente

    while not stop_event.is_set():
        # --- MUDANÇA: Lê a mensagem do dicionário a cada iteração ---
        message = status_data.get('message', 'Carregando')
        dots = chars[idx % len(chars)]
        
        # Prepara a linha para impressão
        line = f"\r{message}{dots}"
        current_line_len = len(line)

        # Adiciona "padding" (espaços em branco) se a linha atual
        # for mais curta que a anterior, para apagar os caracteres antigos.
        padding = " " * max(0, last_line_len - current_line_len)
        
        sys.stdout.write(line + padding)
        sys.stdout.flush()
        
        last_line_len = current_line_len # Guarda o tamanho da linha
        idx += 1
        time.sleep(0.3)
    
    # Limpa a linha ao terminar
    sys.stdout.write("\r" + " " * last_line_len + "\r")
    sys.stdout.flush()

