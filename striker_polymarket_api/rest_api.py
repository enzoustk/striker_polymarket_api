import requests
import pandas as pd
from config import URLS
from helpers import assertion_active
from rest_api.fetch import (
    _fetch_positions_data,
    _fetch_market_data
)


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