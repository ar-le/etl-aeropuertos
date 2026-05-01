import numpy as np
import pandas as pd
from dagster import asset, AssetExecutionContext
from resources.extract_path import GetExtractPathResource


@asset(group_name='extract')
def airport_data_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_airport_data()
    return pd.read_csv(path, sep=';')


@asset(group_name='extract')
def airport_city_state_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_airport_city_state()
    return pd.read_csv(path, sep=';')


@asset(group_name='extract')
def channel_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_channel()
    return pd.read_csv(path, sep=';')


@asset(group_name='extract')
def customer_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_customer()
    return pd.read_csv(path, sep=';')


@asset(group_name='extract')
def fare_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_fare()
    return pd.read_csv(path, sep=';')


@asset(group_name='extract')
def flight_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_flight()
    df = pd.read_csv(path, sep=';')
    #rango fechas
    start_date = pd.to_datetime('2024-01-01')
    end_date = pd.to_datetime('2026-12-31')
    # Calcular la diferencia en días para poder elegir aleatoriamente
    dias_totales = (end_date - start_date).days
    
    # Fechas aleatorias
    random_days = np.random.randint(0, dias_totales, size=len(df))
    df['flight_date'] = start_date + pd.to_timedelta(random_days, unit='D')

    return df


@asset(group_name='extract')
def frequent_flyer_df(context: AssetExecutionContext, extract_path: GetExtractPathResource) -> pd.DataFrame:
    path = extract_path.get_frequent_flyer()
    return pd.read_csv(path, sep=';')