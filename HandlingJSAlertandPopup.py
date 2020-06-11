from selenium import webdriver
class HandlingAlertPopups:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")
        self.driver.get("https://mail.rediff.com/cgi-bin/login.cgi")
    def HandlingAlertPopups(self):
        title =self.driver.title
        assert title== "Rediffmail" ,  "Title is not as Expected"
        self.driver.find_element_by_name("proceed").submit()
        alrt = self.driver.switch_to_alert()
        assert alrt.text=="Please enter a valid user name", "Alert Message is not as expected"
        alrt.accept()
    def teardown(self):
        self.driver.close()
hps = HandlingAlertPopups()
hps.HandlingAlertPopups()
hps.teardown()