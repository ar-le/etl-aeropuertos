import pandas as pd
from dagster import asset, AssetExecutionContext
from pathlib import Path


@asset(group_name='load')
def save_tipo_asiento(context: AssetExecutionContext, tipo_asiento_df: pd.DataFrame):
    """Guarda tipo_asiento_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'tipo_asiento.csv'
    tipo_asiento_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_tipo_pago(context: AssetExecutionContext, tipo_pago_df: pd.DataFrame):
    """Guarda tipo_pago_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'tipo_pago.csv'
    tipo_pago_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_avion(context: AssetExecutionContext, avion_df: pd.DataFrame):
    """Guarda avion_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'avion.csv'
    avion_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_hora(context: AssetExecutionContext, hora_df: pd.DataFrame):
    """Guarda hora_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'hora.csv'
    hora_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_aeropuerto(context: AssetExecutionContext, aeropuerto_df: pd.DataFrame):
    """Guarda aeropuerto_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'aeropuerto.csv'
    aeropuerto_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_pasajero(context: AssetExecutionContext, pasajero_df: pd.DataFrame):
    """Guarda pasajero_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'pasajero.csv'
    pasajero_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path


@asset(group_name='load')
def save_trayecto(context: AssetExecutionContext, trayecto_df: pd.DataFrame):
    """Guarda trayecto_df como CSV en la carpeta prueba"""
    output_path = Path(__file__).parent.parent / 'prueba' / 'trayecto.csv'
    trayecto_df.to_csv(output_path, index=False, sep=';')
    context.log.info(f"Guardado: {output_path}")
    return output_path
