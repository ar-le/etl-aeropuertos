from dagster import ConfigurableResource


class GetExtractPathResource(ConfigurableResource):
    airport_data_path: str
    airport_city_state_path: str
    channel_path: str
    customer_path: str
    fare_path: str
    flight_path: str
    frequent_flyer_path: str


    def get_airport_data(self) -> str:
        return self.airport_data_path

    def get_airport_city_state(self) -> str:
        return self.airport_city_state_path

    def get_channel(self) -> str:
        return self.channel_path

    def get_customer(self) -> str:
        return self.customer_path

    def get_fare(self) -> str:
        return self.fare_path

    def get_flight(self) -> str:
        return self.flight_path

    def get_frequent_flyer(self) -> str:
        return self.frequent_flyer_path