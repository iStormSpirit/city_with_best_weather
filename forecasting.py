import multiprocessing
import time

from tasks import DataAggregationTask, DataAnalyzingTask, DataCalculationTask


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    queue = multiprocessing.Queue()
    calculate = DataCalculationTask(queue)
    to_csv = DataAggregationTask(queue)
    start = time.time()
    calculate.run()
    to_csv.run()
    DataAnalyzingTask().analyze()
    print(f'Время выполнения: {time.time() - start : .2f} s.')


if __name__ == "__main__":
    forecast_weather()
