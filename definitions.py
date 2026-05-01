from dagster import AssetSelection, Definitions, define_asset_job, load_from_defs_folder, load_assets_from_modules
from assets import extract, transform, load
from resources.extract_path import GetExtractPathResource


all_assets = load_assets_from_modules([extract, transform, load])

full_etl = define_asset_job(
    "full_etl",
    selection=AssetSelection.all(),
)



defs = Definitions(
    assets=all_assets,
    resources={
        'extract_path': GetExtractPathResource(
            airport_data_path='data/airport.csv',
            airport_city_state_path='data/airport_city_state.csv',
            channel_path='data/channel.csv',
            customer_path='data/customer.csv',
            fare_path='data/fare.csv',
            flight_path='data/flight.csv',
            frequent_flyer_path='data/frequentflyer.csv'
        )
    },
    jobs=[full_etl]
)