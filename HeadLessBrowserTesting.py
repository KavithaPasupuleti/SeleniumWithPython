from selenium import webdriver
from selenium.webdriver.chrome.options import Options
class HeadLessBrowserTesting:
    def __init__(self):
        self.chrome_options=Options()
        self.chrome_options.add_argument("--headless")
        self.url2="https://mail.rediff.com/cgi-bin/login.cgi"
        self.driver = webdriver.Chrome\
            (executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver",
             chrome_options=self.chrome_options)

    def Navigations_and_Screenshot(self):
        self.driver.get(self.url2)
        print(self.driver.title)
        config = configparser.ConfigParser()

    def teardown(self):
        self.driver.close()
hps = HeadLessBrowserTesting()
hps.Navigations_and_Screenshot()
hps.teardown()