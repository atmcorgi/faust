#!/usr/bin/env python
import faust
import random
import datetime

# Khởi tạo ứng dụng Faust
app = faust.App('weather-stream', broker='kafka://localhost:9092')

# Định nghĩa kiểu dữ liệu WeatherData
class WeatherData(faust.Record, serializer='json'):
    city: str
    temperature: float
    humidity: float
    wind_speed: float
    timestamp: str

# Tạo topic Kafka
weather_topic = app.topic('weather', value_type=WeatherData)

# Bảng lưu tổng nhiệt độ và số lần cập nhật để tính trung bình chính xác
temperature_sums = app.Table('temperature_sums', default=float)
temperature_counts = app.Table('temperature_counts', default=int)
average_temperatures = app.Table('average_temperatures', default=float)

# Bảng lưu trữ dữ liệu thời tiết mới nhất theo thành phố
latest_weather = app.Table('latest_weather', default=dict)

# Logic xử lý dữ liệu thời tiết
@app.agent(weather_topic)
async def process_weather(stream):
    async for weather_data in stream:
        city = weather_data.city

        # Cập nhật dữ liệu mới nhất
        latest_weather[city] = weather_data.asdict()

        # Cập nhật tổng nhiệt độ và số lần nhận dữ liệu
        temperature_sums[city] += weather_data.temperature
        temperature_counts[city] += 1

        # Tính nhiệt độ trung bình chính xác
        average_temperatures[city] = temperature_sums[city] / temperature_counts[city]

        # In thông tin dữ liệu nhận được
        print(f"Received data - City: {city}, "
              f"Temperature: {weather_data.temperature:.2f}°C, "
              f"Humidity: {weather_data.humidity:.2f}%, "
              f"Wind Speed: {weather_data.wind_speed:.2f} m/s, "
              f"Timestamp: {weather_data.timestamp}")

# API hiển thị nhiệt độ trung bình của từng thành phố
@app.page('/average_temperatures/')
async def get_avg_temperatures(web, request):
    return web.json({city: temp for city, temp in average_temperatures.items()})

# API hiển thị dữ liệu thời tiết mới nhất
@app.page('/latest_weather/')
async def get_latest_weather(web, request):
    return web.json({
        city: latest_weather[city] if latest_weather[city] else None
        for city in latest_weather.keys()
    })

# Sử dụng timer để gửi dữ liệu thời tiết ngẫu nhiên mỗi 5 giây
@app.timer(5)
async def produce():
    cities = ['Hanoi', 'HoChiMinhCity', 'Danang']
    for city in cities:
        temperature = random.uniform(20, 40)
        humidity = random.uniform(50, 100)
        wind_speed = random.uniform(0, 15)
        timestamp = str(datetime.datetime.now())

        weather_data = WeatherData(
            city=city,
            temperature=temperature,
            humidity=humidity,
            wind_speed=wind_speed,
            timestamp=timestamp
        )

        await weather_topic.send(value=weather_data)

# Chạy ứng dụng Faust
if __name__ == '__main__':
    app.main()

