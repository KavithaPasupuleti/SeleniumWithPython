from pyjavaproperties import Properties
from selenium import webdriver
class HandlingObjectRepository:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")
        self.p = Properties()
        self.p.load(open('/home/easyway/Music/github/SeleniumWithPython/config.properties'))
        self.p.list()
    def Handling_Object_Repository(self):
        self.driver.get(self.p["url"])
        print(self.driver.title)
        assert self.driver.title==self.p["title"], "Title is not as expected"

    def teardown(self):
        self.driver.close()
hps = HandlingObjectRepository()
hps.Handling_Object_Repository()
hps.teardown()