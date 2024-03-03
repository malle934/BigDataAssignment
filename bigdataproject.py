import requests
import redis
import json
import matplotlib.pyplot as plt

class DataProcessor:
    """
    Class performs the operation of reading data from api as json and insert into redis and read from redis 
    and perform search , aggregation and plot operation
    
    """
    def __init__(self):
        self.redis_client = redis.Redis(host='redis-10097.c251.east-us-mz.azure.cloud.redislabs.com',port=10097,
                                         password='wH6gHKl5CRL7JhURnVLm26CKPom0r2K3')
    
    def fetch_data_from_api(self, api_url):
        """
        It fetch data from api
        Args:
            api_url: String 
        Returns:
               returns the json
        """
        response = requests.get(api_url)
        return response.json()

    def store_data_in_redis(self, key, data):
         """
        It stores data into redis
        Args:
            key: String
            data: dict
        """
         self.redis_client.set(key, json.dumps(data))

    def read_data_from_redis(self, key):
        """
        It reads the data from redis
        Args:
            key: String 
        Returns:
               returns the data read from redis
        """
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    def plot_top_currencies(self, data, num_currencies=10):
        """
        It creats plot of top 10 least currencies
        Args:
            data: dict
            num_currencies: int
        Returns:
               returns the plot of currency vs rate
        """
        sorted_rates = sorted(data['rates'].items(), key=lambda x: x[1], reverse=True)
        top_currencies = dict(sorted_rates[:num_currencies])
        plt.bar(top_currencies.keys(), top_currencies.values())
        plt.xlabel('Currency')
        plt.ylabel('Rate')
        plt.title('Top 10 Least Currencies by U.S.D Rate')
        plt.xticks(rotation=45)
        plt.show()
    
    def find_maximum_rate(self, data):
        """
        It finds the maximum rate
        Args:
            data: dict 
        Returns:
               returns the max rate and max currency
        """
        if not data:
            return None
        max_rate = max(data.values())
        max_currency = [currency for currency, rate in data.items() if rate == max_rate]
        return max_currency[0], max_rate
    
    def convert_to_usd(self, data):
        """
        It convertes currency into usd
        Args:
            data: dict 
        Returns:
               returns the converted rates
        """
        if not data or 'rates' not in data:
            return None
        usd_rate = data['rates']['USD']
        converted_rates = {currency: rate / usd_rate for currency, rate in data['rates'].items()}
        return converted_rates
    
    def find_minimum_rate(self, data):
        """
        It finds the minimum rate
        Args:
            data: dict 
        Returns:
               returns the minimum rate and max currency
        """
        if not data:
            return None
        
        min_rate = min(data.values())
        min_currency = [currency for currency, rate in data.items() if rate == min_rate]
        return min_currency[0], min_rate

    def search_data(self, data, keyword):
        """
        It searches the data in redis
        Args:
            data: dict 
            keyword: string
        Returns:
               returns the value which found
        """
        matched_results = [rate for rate in data if keyword.lower() in rate.lower()]
        for rate in matched_results:
            print(f"Found {rate}")


# Example usage:
if __name__ == "__main__":
    api_url = "https://open.er-api.com/v6/latest/USD"
    processor = DataProcessor()

    # Fetch data from API
    data = processor.fetch_data_from_api(api_url)
    
    # Store data in Redis
    processor.store_data_in_redis('exchange_rates', data)

    # Read data from Redis
    data_from_redis = processor.read_data_from_redis('exchange_rates')
    usd_data = processor.convert_to_usd(data_from_redis)
    # Perform processing
    if data_from_redis:
        processor.plot_top_currencies(data_from_redis) 
        processor.search_data(data_from_redis['rates'], 'INR')
        if usd_data:
            min_currency, min_rate = processor.find_maximum_rate(usd_data)
            max_currency, max_rate = processor.find_minimum_rate(usd_data)
            print("Highest currency:", max_currency, max_rate)
            print("Lowest currency :", min_currency, min_rate)
        else:
            print("Unable to convert currency rates to USD.")
        
    else:
        print("No data found in Redis.")
