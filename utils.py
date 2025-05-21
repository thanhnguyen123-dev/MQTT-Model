import time

class Utils:
    @staticmethod
    def format_time(timestamp: float) -> str:
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        return formatted_time