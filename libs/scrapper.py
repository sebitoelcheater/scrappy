import pandas as pd


class Scrapper:
    @classmethod
    def scraps(cls, soups, scrap_fun):
        data = pd.DataFrame()
        for soup in soups:
            dct = scrap_fun(soup)
            if dct is not None:
                data = data.append(dct)
        return data
