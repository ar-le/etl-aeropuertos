import pandas as pd
from dagster import asset, AssetExecutionContext
from resources.hive_resource import HiveResource



@asset(group_name='load')
def save_tipo_asiento(context: AssetExecutionContext, tipo_asiento_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda tipo_asiento_df en Hive"""
    engine = hive_resource.get_engine()
    tipo_asiento_df.to_sql(
        name="tipo_asiento",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("tipo_asiento guardado en Hive")


@asset(group_name='load')
def save_tipo_pago(context: AssetExecutionContext, tipo_pago_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda tipo_pago_df en Hive"""
    engine = hive_resource.get_engine()
    tipo_pago_df.to_sql(
        name="tipo_pago",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("tipo_pago guardado en Hive")




#
@asset(group_name='load')
def save_avion(context: AssetExecutionContext, avion_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda avion_df en Hive"""
    engine = hive_resource.get_engine()
    avion_df.to_sql(
        name="avion",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("avion guardado en Hive")


@asset(group_name='load')
def save_hora(context: AssetExecutionContext, hora_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda hora_df en Hive"""
    engine = hive_resource.get_engine()
    hora_df.to_sql(
        name="hora",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("hora guardado en Hive")


@asset(group_name='load')
def save_aeropuerto(context: AssetExecutionContext, aeropuerto_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda aeropuerto_df en Hive"""
    engine = hive_resource.get_engine()
    aeropuerto_df.to_sql(
        name="aeropuerto",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("aeropuerto guardado en Hive")


@asset(group_name='load')
def save_pasajero(context: AssetExecutionContext, pasajero_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda pasajero_df en Hive"""
    engine = hive_resource.get_engine()
    pasajero_df.to_sql(
        name="pasajero",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("pasajero guardado en Hive")


@asset(group_name='load')
def save_trayecto(context: AssetExecutionContext, trayecto_df: pd.DataFrame, hive_resource: HiveResource):
    """Guarda trayecto_df en Hive"""
    engine = hive_resource.get_engine()
    trayecto_df.to_sql(
        name="trayecto",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    context.log.info("trayecto guardado en Hive")
