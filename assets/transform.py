import pandas as pd
from dagster import asset, AssetExecutionContext

@asset(group_name='transform')
def tipo_asiento_df(context: AssetExecutionContext, fare_df: pd.DataFrame) -> pd.DataFrame:
    df = fare_df[['fare_class_key', 'fare_class_description']]
    df.rename(columns={'fare_class_key': 'tipo_asiento_id', 'fare_class_description': 'descripcion'}, inplace=True)
    return df 


@asset(group_name='transform')
def tipo_pago_df(context: AssetExecutionContext, channel_df: pd.DataFrame) -> pd.DataFrame:
    df = channel_df
    df.rename(columns={'channel_key': 'tipo_pago_id', 'channel_name': 'descripcion'}, inplace=True)
    return df


@asset(group_name='transform')
def avion_df(context: AssetExecutionContext, flight_df: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Transformando datos de aviones")
    context.log.info(flight_df.head())
    df = flight_df[['airplane_type', 'seat_capacity']].drop_duplicates().reset_index(drop=True)
    #generar ID numérico
    df['avion_id'] = df.index + 1
    df.rename(columns={'airplane_type': 'tipo', 'seat_capacity': 'capacidad'}, inplace=True)
    return df

# Función para clasificar el momento del día
def clasificar_momento_dia(hora):
    if 6 <= hora < 12:
        return 'mañana'
    elif 12 <= hora < 20:
        return 'tarde'
    else:
        return 'noche'

@asset(group_name='transform')
def hora_df(context: AssetExecutionContext) -> pd.DataFrame:
    # Generar un df con todas las combinaciones posibles de hora y minuto del día
    horas = list(range(24))
    minutos = list(range(60))
    df = pd.DataFrame([(h, m) for h in horas for m in minutos], columns=['hora', 'minuto'])
    
    df['momento_dia'] = df['hora'].apply(clasificar_momento_dia)
    # Añadir id numérico
    df['hora_id'] = df.index + 1
    
    return df


@asset(group_name='transform')
def aeropuerto_df(context: AssetExecutionContext, airport_data_df: pd.DataFrame, airport_city_state_df: pd.DataFrame) -> pd.DataFrame:
    df = airport_data_df.merge(airport_city_state_df, left_on='leg_city', right_on='city', how='left')
    df.rename(columns={'leg_key': 'aeropuerto_id', 'leg_name': 'nombre', 'leg_city': 'ciudad', 'state': 'estado', 
                       'leg_type': 'tipo_aeropuerto', 'leg_radar_type': 'tipo_radar'}, 
                       inplace=True)
    return df


@asset(group_name='transform')
def pasajero_df(context: AssetExecutionContext, customer_df: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Transformando datos de pasajeros")
    context.log.info(customer_df.head())
    df = customer_df[['customer_key', 'customer_city', 'customer_state', 'customer_type', 'customer_income', 'customer_birth_date', 'customer_sex']]
    #de la fecha de nacimineto mantener solo el año (está en formato marzo 12, 1956)
    df['customer_birth_year'] = df['customer_birth_date'].str.extract(r'(\d{4})')[0].astype(int)
    df.drop(columns=['customer_birth_date'], inplace=True)

    #customer_income pasa a clasificarse por rangos en tres columnas binarias: nivel_ingresos_bajo, nivel_ingresos_medio, nivel_ingresos_alto. El rango bajo es menor a 40000, el medio entre 40000 y 80000, y el alto mayor a 80000
    #primero eliminar el simbolo $ y el punto de los miles, y convertir a float
    df['customer_income'] = df['customer_income'].str.replace('$', '').str.replace('.', '').astype(float)
    df['nivel_ingresos_bajo'] = (df['customer_income'] < 40000).astype(int)
    df['nivel_ingresos_medio'] = ((df['customer_income'] >= 40000) & (df['customer_income'].astype(float) <= 80000)).astype(int)
    df['nivel_ingresos_alto'] = (df['customer_income'] > 80000).astype(int)
    df.drop(columns=['customer_income'], inplace=True)

    #columna flag_Activo por si se modifican datos de los clientes en un futuro. Por ahora todos los clientes están activos, por lo que se asigna el valor 1 a todos
    df['flag_activo'] = 1

    #Renombrar columnas
    df.rename(columns={'customer_key': 'pasajero_id', 'customer_city': 'ciudad', 'customer_state': 'estado', 'customer_type': 'tipo','customer_sex': 'genero', 'customer_birth_year': 'anyo_nacimiento'}, inplace=True)

    return df


@asset(group_name='transform')
def trayecto_df(context: AssetExecutionContext, frequent_flyer_df: pd.DataFrame, avion_df: pd.DataFrame, flight_df: pd.DataFrame, hora_df: pd.DataFrame, tipo_asiento_df: pd.DataFrame, tipo_pago_df: pd.DataFrame) -> pd.DataFrame:
    #Columnas que no hay que transformar
    df = frequent_flyer_df[['ticket_number', 'customer_key','fare_class_key', 'channel_key', 'fare', 'miles', 'minutes_late', 'leg_origin_key', 'leg_dest_key', 'trip_origin_key', 'trip_dest_key', 'flight_key']]

    #Horas de salida y llegada
    #Obtener claves de tabla hora para sched_depart y sched_arrival
    df = df.merge(flight_df[['flight_key','sched_depart', 'sched_arrival','flight_date']], left_on='flight_key', right_on='flight_key',  how='left')
    #Convertir sched_depart y sched_arrival a formato datetime para extraer hora y minuto
    df['sched_depart'] = pd.to_datetime(df['sched_depart'], format='%H:%M')
    df['sched_arrival'] = pd.to_datetime(df['sched_arrival'], format='%H:%M')
    df['depart_hora'] = df['sched_depart'].dt.hour
    df['depart_minuto'] = df['sched_depart'].dt.minute
    df['arrival_hora'] = df['sched_arrival'].dt.hour
    df['arrival_minuto'] = df['sched_arrival'].dt.minute
    #joins para obtener key de la hora de salida y llegada
    df = df.merge(hora_df[['hora_id','hora','minuto']], left_on=['arrival_hora', 'arrival_minuto'], right_on=['hora', 'minuto'], how='left')
    #renombrar la columna hora_id a arrival_hora_id
    df.rename(columns={'hora_id': 'llegada_hora_id'}, inplace=True)
    df = df.merge(hora_df[['hora_id','hora','minuto']], left_on=['depart_hora', 'depart_minuto'], right_on=['hora', 'minuto'], how='left')
    #renombrar la columna hora_id a depart_hora_id
    df.rename(columns={'hora_id': 'salida_hora_id'}, inplace=True)
    df.drop(columns=['sched_depart', 'sched_arrival', 'depart_hora', 'depart_minuto', 'arrival_hora', 'arrival_minuto', 'hora_x', 'minuto_x', 'hora_y', 'minuto_y'], inplace=True)

    #Avion - buscar en avion_df por airplane type (que está en flight_df) para obtener el avion_id
    df = df.merge(flight_df[['flight_key', 'airplane_type']], left_on='flight_key', right_on='flight_key', how='left')
    df = df.merge(avion_df[['avion_id', 'tipo']], left_on='airplane_type', right_on='tipo', how='left')
    df.drop(columns=['airplane_type', 'tipo'], inplace=True)


    #tarifa y pago - hay claves que están en frequent flyer pero no existen en las tablas de tipo_asiento y tipo_pago, por lo que se hace un left join para obtener la descripción si existe. Si no existe se elimina la clave de la tabla principal
    df = df.merge(tipo_asiento_df[['tipo_asiento_id', 'descripcion']], left_on='fare_class_key', right_on='tipo_asiento_id', how='left')
    df = df.merge(tipo_pago_df[['tipo_pago_id', 'descripcion']], left_on='channel_key', right_on='tipo_pago_id', how='left')
    #Poner a null las claves de tipo_asiento y tipo_pago si no se encontró la descripción
    df.loc[df['descripcion_x'].isnull(), 'fare_class_key'] = None
    df.loc[df['descripcion_y'].isnull(), 'channel_key'] = None
    df.drop(columns=['descripcion_x', 'descripcion_y', 'tipo_asiento_id', 'tipo_pago_id'], inplace=True)

    #Renombrar columnas
    df.rename(columns={'customer_key': 'pasajero_id', 'fare_class_key': 'tipo_asiento_id', 'channel_key': 'tipo_pago_id', 'fare': 'tarifa', 'miles': 'millas', 'minutes_late': 'minutos_retraso', 'leg_origin_key': 'origen_id', 'leg_dest_key': 'destino_id', 'trip_origin_key': 'origen_viaje_id', 'trip_dest_key': 'destino_viaje_id', 'flight_key': 'vuelo_id'}, inplace=True)
    
    #Convertir a Int64 (nullable integer) para mantener NaN sin convertir a float
    #Si no para poder tener nulls se pasa a a float, con valores 1.0
    df['tipo_asiento_id'] = df['tipo_asiento_id'].astype('Int64')
    df['tipo_pago_id'] = df['tipo_pago_id'].astype('Int64')
    

    return df


