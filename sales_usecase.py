import pandas as pd
import requests

class SalesDataProcessor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
        
    def get_weather_data(self, location):
        params = {
            "q": location,
            "appid": self.api_key,
            "units": "metric" 
        }
        response = requests.get(self.api_endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("Failed to fetch weather data:", response.status_code)
            return None
    
    def process_sales_with_weather(self, sales_data, users_data):
        user_mapping = {}
        for user in users_data:
            user_mapping[user['id']] = {
                'name': user['name'],
                'username': user['username'],
                'email': user['email'],
                'lat': user['address']['geo']['lat'],
                'lng': user['address']['geo']['lng']
            }

        sales_with_weather = []
        for sale in sales_data:
            user_info = user_mapping.get(sale['customer_id'])
            if user_info:
                location = f"{user_info['lat']},{user_info['lng']}"
                weather_data = self.get_weather_data(location)
                if weather_data:
                    sale['weather'] = {
                        'temperature': weather_data['main']['temp'],
                        'conditions': weather_data['weather'][0]['description']
                    }
                else:
                    sale['weather'] = {
                        'temperature': None,
                        'conditions': None
                    }
                sale.update(user_info)
                sales_with_weather.append(sale)
            else:
                print(f"User with ID {sale['customer_id']} not found.")
        
        return pd.DataFrame(sales_with_weather)

# Load sales data and user data
sales_data = pd.read_csv('sales_data.csv')
users_response = requests.get('https://jsonplaceholder.typicode.com/users')
users_data = users_response.json()

# Initialize SalesDataProcessor
api_key = "YOUR_API_KEY"
processor = SalesDataProcessor(api_key)

# Process sales data with weather information
sales_with_weather = processor.process_sales_with_weather(sales_data, users_data)

# Aggregations and data manipulations
# 1. Calculate total sales amount per customer
total_sales_per_customer = sales_with_weather.groupby('name')['price'].sum()

# 2. Determine the average order quantity per product
average_order_quantity_per_product = sales_with_weather.groupby('product')['quantity'].mean()

# 3. Identify the top-selling products
top_selling_products = sales_with_weather.groupby('product')['quantity'].sum().nlargest(5)

# 4. Identify the top-selling customers
top_selling_customers = sales_with_weather.groupby('name')['quantity'].sum().nlargest(5)

# 5. Analyze sales trends over time (e.g., monthly or quarterly sales)
sales_with_weather['order_date'] = pd.to_datetime(sales_with_weather['order_date'])
monthly_sales_trend = sales_with_weather.resample('M', on='order_date')['price'].sum()

# 6. Include weather data in the analysis
average_sales_per_weather_condition = sales_with_weather.groupby('conditions')['price'].mean()

# Display results
print("Total Sales Amount per Customer:", total_sales_per_customer)
print("Average Order Quantity per Product:", average_order_quantity_per_product)
print("Top Selling Products:", top_selling_products)
print("Top Selling Customers:", top_selling_customers)
print("Monthly Sales Trend:", monthly_sales_trend)
print("Average Sales Amount per Weather Condition:" , average_sales_per_weather_condition)

