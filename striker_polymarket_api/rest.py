import requests
import pandas as pd
from helpers import safe_divide
from rest_api.clv import fetch_clv
from helpers import assertion_active
from striker_polymarket_api.config import URLS
from rest_api.price_history import process_dataframe
from rest_api.fetch import (
    _fetch_positions_data,
    _fetch_market_data
)


def calculate_clv(
    user_address: str,
    df: pd.DataFrame,
    ) -> pd.DataFrame:
    
    print("--- INICIANDO calculate_clv ---")
    
    df = df.copy()
    
    # Colocar o df na forma correta
    clv_df = process_dataframe(df)
    
    clv_results = {}
    clv_reasons = {}
    
    def create_key(df: pd.DataFrame) -> pd.Series:
        return df['conditionId'].astype(str) + '_' + df['asset'].astype(str)
    
    clv_df['_composite_key'] = create_key(df=clv_df)
    
    try:
        clv_df['start_time_unix'] = pd.to_datetime(
            clv_df['start_time'], format='ISO8601', utc=True
        ).astype('int64') // 10**9
        print(f"DataFrame principal (df) preparado. {len(df)} linhas.")
        print(f"Exemplo start_time_unix: {clv_df['start_time_unix'].iloc[0]}")
    
    except Exception as e:
        print(f"Erro CRÍTICO ao converter 'start_time' no df principal: {e}")
        return clv_df # Retorna o df original se falhar aqui

    df_lookup = clv_df.set_index(['conditionId', 'asset']).sort_index()
    
    print("Buscando trades da API (fetch_clv)...")
    trades_df = fetch_clv.fetch_clv(
        user_address=user_address,
        df=df
    )
    
    if trades_df.empty:
        print("❌ ERRO: fetch_clv retornou um DataFrame VAZIO. Nenhum trade para processar.")
        print("--- FINALIZANDO calculate_clv (sem dados) ---")
        clv_df = clv_df.drop(columns=['start_time_unix', '_composite_key'], errors='ignore')
        return clv_df
        
    print(f"✅ Trades recebidos. Shape do trades_df: {trades_df.shape}")

    try:
        # *** DEBUG: Vamos inspecionar o timestamp ANTES de converter ***
        raw_timestamp_example = trades_df['timestamp'].iloc[0]
        print(f"Exemplo de 'timestamp' RAW da API: {raw_timestamp_example} (Tipo: {type(raw_timestamp_example)})")
        
        trades_df['timestamp_seconds'] = trades_df['timestamp'].astype(float) // 1000
        
        print(f"Exemplo de 'timestamp_seconds' CONVERTIDO: {trades_df['timestamp_seconds'].iloc[0]}")
        
    except KeyError:
        print("❌ ERRO: A coluna 'timestamp' não foi encontrada no trades_df.")
        clv_df = clv_df.drop(columns=['start_time_unix', '_composite_key'], errors='ignore')
        return clv_df
    
    except Exception as e:
        print(f"❌ ERRO ao converter timestamp do trades_df: {e}")
        clv_df = clv_df.drop(columns=['start_time_unix', '_composite_key'], errors='ignore')
        return clv_df

    trades_df['_composite_key'] = create_key(df=trades_df)
    df_keys_set = set(zip(clv_df['conditionId'], clv_df['asset']))
    
    print(f"--- Iniciando loop por {len(trades_df.groupby(['conditionId', 'asset']))} grupos de trades ---")
    
    # Entrar no loop e calcular o CLV para todas as apostas
    for (condition_id, asset), group_df in trades_df.groupby(['conditionId', 'asset']):
        composite_key =  f"{condition_id}_{asset}"
                    
        try:
            if (condition_id, asset) not in df_keys_set:
                print("  -> SKIP: Chave não encontrada no df principal.")
                continue
                
            lookup_row = df_lookup.loc[(condition_id, asset)]
            
            if isinstance(lookup_row, pd.DataFrame):
                lookup_row = lookup_row.iloc[0]
            
            # *** DEBUG: Inspecionar os valores de lookup ***
            start_time = lookup_row['start_time_unix']
            closing_price = lookup_row['match_start_price']
            
            # *** DEBUG: Checar se o closing_price é NaN ***
            if pd.isna(closing_price):
                print("  -> ERRO: 'match_start_price' (Closing Price) é NaN. Pulando.")
                clv_reasons[composite_key] = 'closing_price_is_nan'
                continue
                            
            # Usar a coluna 'timestamp_seconds' para o filtro
            filtered_df = group_df[group_df['timestamp_seconds'] < start_time].copy()
                            
            total_size = filtered_df['size'].sum()
            
            if total_size == 0:
                print("  -> SKIP: Nenhum trade encontrado antes do start_time (Total Size = 0).")
                clv_reasons[composite_key] = 'sem_trades_pre_market'
                continue 

            # Calcular agora o preço médio.
            avg_price = (
                (filtered_df['size'] * filtered_df['price']).sum() 
                / total_size
                )
            
            price_clv = closing_price - avg_price

            odds_clv = safe_divide(1,avg_price) - safe_divide(1,closing_price)
        
            
            clv_results[composite_key] = {
                'price_clv': price_clv, 'odds_clv': odds_clv,
                'avg_price': avg_price, 'closing_price': closing_price
            }
            
        except Exception as e:
            print(f"  -> ERRO CRÍTICO no loop: {e}")
            clv_reasons[composite_key] = f'erro_processamento: {str(e)[:50]}'
                            
    print("\n--- Loop finalizado. Mapeando resultados ---")
    
    price_clv_map = {key: val['price_clv'] for key, val in clv_results.items()}
    odds_clv_map = {key: val['odds_clv'] for key, val in clv_results.items()}

    clv_df['price_clv'] = clv_df['_composite_key'].map(price_clv_map)
    clv_df['odds_clv'] = clv_df['_composite_key'].map(odds_clv_map)
            
    clv_df = clv_df.drop(
        columns=['start_time_unix', '_composite_key'], errors='ignore'
        )
    
    # Imprimir um resumo dos problemas
    if clv_reasons:
        print("\nRelatório de CLV (itens não calculados):")
        reason_counts = pd.Series(clv_reasons).value_counts()
        print(reason_counts)
    
    print("Estatísticas do CLV:")
    print("\n" + "-" * 40)
    print("-" * 40)
    
    # 1. Isolar os valores de CLV que foram calculados com sucesso (não-nulos)
    valid_clv = clv_df['price_clv'].dropna()
    total_calculated = len(valid_clv)
    
    # 2. Verificar se temos dados para calcular
    if total_calculated == 0:
        print("  Nenhum valor de CLV foi calculado com sucesso.")
        print("  Estatísticas indisponíveis.")

    return clv_df


def number_of_trades(
    user_address
    ) -> int:
    """
    Busca o número total de trades de um usuário (com animação simples).
    """
    params = {"user": user_address}
    
    try:
        response = requests.get(URLS['TOTAL_TRADES'], params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "traded" in data:
            return data["traded"]
        else: 
            return -1

    except Exception as e:
        return -1


def fetch_live_positions(
    user_address: str
    ) -> pd.DataFrame:    
    # Wrapper para retornar trades ao vivo
    return pd.DataFrame(_fetch_positions_data(
            user_address=user_address,
            url=URLS['ACTIVE_POSITIONS'],
            num_processes=1,
            display_message="Fetching Live Positions"
        ))


def fetch_closed_positions(
    user_address: str
    )-> pd.DataFrame:
    # Wrapper para retornar trades fechados
    return pd.DataFrame(_fetch_positions_data(
        user_address=user_address,
        url=URLS['CLOSED_POSITIONS'],
        num_processes=20,
        display_message="Fetching Closed Positions"
    ))


def fetch_all_positions(
    user_address: str,
    ):
    """
    Fetch closed and live positions for a given user address
    """    
    
    # Closed Positions
    closed_data = fetch_closed_positions(user_address)
    
    # Active Positions
    active_data = fetch_live_positions(user_address)

    # Asserts Live Positions are indeed live
    combined_df = assertion_active(active_data, closed_data)

    # Gets market information for each of the trades
    complete_df = _fetch_market_data(combined_df)
    
    return complete_df