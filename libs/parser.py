import datetime
import re
import pytz


class Parser:
    def __init__(self, raw_dict):
        self.raw_dict = raw_dict
        self.final_value = {}

    def parse(self, key):
        method_to_call = getattr(self, key)
        if method_to_call is None:
            raise NotImplemented
        value = method_to_call()
        if value is not None:
            self.final_value[key] = value

    def get_value(self):
        return self.final_value

    def parse_date(self, str_date, date_format="%Y-%m-%d %H:%M:%S"):
        local_time = pytz.timezone("America/New_York")
        naive_datetime = datetime.datetime.strptime(str_date, date_format)
        local_datetime = local_time.localize(naive_datetime, is_dst=None)
        return local_datetime.astimezone(pytz.utc)

    def search(self, text, term):
        return re.search(
            re.sub(r'\W+', '', text).lower(),
            re.sub(r'\W+', '', term).lower()
        )
