from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoAlertPresentException, UnexpectedAlertPresentException
from selenium.webdriver.support.wait import WebDriverWait


class Scrappa:
    def _get(self, link, wait=True):
        # self._set_driver()
        self.driver.stop_client()
        try:
            self.driver.get('about:blank')
        except UnexpectedAlertPresentException as e:
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
        try:
            self.driver.get(link)
            if wait:
                return self.driver.page_source
        except UnexpectedAlertPresentException as e:
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
            try:
                if wait:
                    return self.driver.page_source
            except TimeoutException as e:
                self._set_driver()
                return self._get(link, wait)
        except TimeoutException as e:
            self._set_driver()
            return self._get(link, wait)

    def _set_driver(self):
        if hasattr(self, 'driver') and self.driver is not None:
            self.driver.quit()
        self.driver = self.browser(**self.browser_args)
        self.driver.set_page_load_timeout(self.timeout)
        self.driver.implicitly_wait(self.timeout)
        self.wait = WebDriverWait(self.driver, self.timeout)

    def _reload(self):
        self.driver.stop_client()
        self.driver.start_client()

    def __init__(self, timeout=5, browser=webdriver.Chrome, browser_args=None, whitelist_domains=None):
        self.timeout = timeout
        self.browser = browser
        default_browser_args = {}
        if browser == webdriver.Chrome:
            chrome_options = webdriver.ChromeOptions()
            prefs = {'profile.managed_default_content_settings.images': 2}
            chrome_options.add_experimental_option("prefs", prefs)
            if whitelist_domains is not None:
                chrome_options.add_argument(
                    f"--host-resolver-rules=MAP * 127.0.0.1, " + ", ".join([f"EXCLUDE {domain}" for domain in whitelist_domains])
                )
            default_browser_args['chrome_options'] = chrome_options
        elif browser == webdriver.Firefox:
            firefox_profile = webdriver.FirefoxProfile()
            firefox_profile.set_preference('permissions.default.image', 2)
            firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
            default_browser_args['firefox_profile'] = firefox_profile
        self.browser_args = default_browser_args if browser_args is None else browser_args
        self._set_driver()

    def soup(self, link):
        page_source = self._get(link)
        return BeautifulSoup(page_source, 'lxml')

    def soups(self, links):
        for link in links:
            yield self.soup(link)

    def get_soup(self):
        return BeautifulSoup(self.driver.page_source, 'lxml')
