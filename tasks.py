from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process

import pandas

from api_client import YandexWeatherAPI
from utils import CITIES, logger


class DataFetchingTask:
    """
    Получает все данные из API Yandex.weather
     """

    @staticmethod
    def get_response(city_name: str) -> dict:
        yw_api = YandexWeatherAPI()
        data = yw_api.get_forecasting(city_name)
        return data


class DataCalculationTask(Process):
    """
    Вычисляет среднюю температуру за день,
    среднюю температуру за указанный промежуток времени,
    сумму часов без осадков за указанный промежуток времени.
    """

    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    @staticmethod
    def day_temperature(city: str) -> dict:
        temp_list = []
        dft = DataFetchingTask()
        response: dict = dft.get_response(city)
        for data in response['forecasts']:
            city = response['geo_object']['locality']['name']
            date = data['date']
            weather = [(i['temp'], i['condition']) for i in data['hours']]
            if not len(weather) < 24:
                temp_list.append(
                    {date: [weather[i] for i in range(9, 20)]}
                )
        return {city: temp_list}

    @staticmethod
    def weather_condition(conditions: list) -> int:
        no_precipitation = ('clear', 'partly-cloud', 'cloudy', 'overcast')
        return len([item for item in conditions if item in no_precipitation])

    def calculate(self, city: str) -> dict:
        all_weather = []
        all_temperature = []
        all_condition = []
        city_data = self.day_temperature(city)
        for values in city_data.values():
            for date in values:
                for weather in date.values():
                    temperature, condition = list(zip(*weather))
                    avg_day_temp = sum(temperature) // len(temperature)
                    all_temperature.append(avg_day_temp)
                    no_precipitation = self.weather_condition(condition)
                    all_condition.append(no_precipitation)
                    current_date = list(date.keys())[0]
                    weather = {'average_temp': avg_day_temp, 'no_precipitation': no_precipitation}
                    all_weather.append(
                        {'date': current_date, 'weather': weather}
                    )
                avg_city_temp = sum(all_temperature) // len(all_temperature)
                city_name = list(city_data.keys())[0]
                city_weather = {
                    'city': city_name,
                    'all_weather': all_weather,
                    'avg_temp': avg_city_temp,
                    'no_precipitation': sum(all_condition),
                    'points': sum(all_temperature, sum(all_condition))
                }
                logger.info(f' def average: получены данные - {city_weather}')
                return city_weather

    def run(self):
        with ThreadPoolExecutor() as executor:
            future = executor.map(self.calculate, CITIES)
            for item in future:
                self.queue.put(item)
                logger.info(f'executor отправил в очередь {item}')


class DataAggregationTask(Process):
    """
    Объединение и запись донных в csv
    """

    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        data = []
        while True:
            if self.queue.empty():
                logger.info('Очередь пуста')
                break
            item = self.queue.get()
            logger.info(f'Из очереди получен элемент {item}')
            data.append(item)
            pdf = pandas.DataFrame.from_records(data)
            pdf.to_csv('output.csv', mode='w', index=False)


class DataAnalyzingTask:
    """
    Анализ полученных данных и вывод о наиболее благоприятном городе для поездки
    """

    @staticmethod
    def analyze():
        df = pandas.read_csv('output.csv', usecols=['city', 'points'])
        result = df.sort_values(by=['points'])
        i = len(result) - 1
        city_to_go = result.iloc[i]
        print(f'Рекомендую поехать в {city_to_go["city"]}')
