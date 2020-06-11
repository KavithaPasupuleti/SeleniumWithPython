from selenium import webdriver
from selenium import webdriver
class NavigationsandScreenshot:
    def __init__(self):
        self.url1="https://www.spicejet.com/"
        self.url2="https://mail.rediff.com/cgi-bin/login.cgi"
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")

    def Navigations_and_Screenshot(self):
        self.driver.get(self.url1)
        title =self.driver.title
        assert title == "SpiceJet - Flight Booking for Domestic and International, Cheap Air Tickets", "Title is not as Expected"
        self.driver.get(self.url2)
        title = self.driver.title
        assert title== "Rediffmail" ,  "Title is not as Expected"
        self.driver.back()
        title = self.driver.title
        assert title == "SpiceJet - Flight Booking for Domestic and International, Cheap Air Tickets", \
            "Back Method is not working as expected"
        self.driver.forward()
        title = self.driver.title
        assert title == "Rediffmail", "Forward method is not working as Expected"
        self.driver.save_screenshot("/home/easyway/Music/github/SeleniumWithPython/savescreenshot.png")
    def teardown(self):
        self.driver.close()
hps = NavigationsandScreenshot()
hps.Navigations_and_Screenshot()
hps.teardown()