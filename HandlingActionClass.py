from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
class HandlingActionClass:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")
        self.driver.maximize_window()
        #self.driver.get("https://mail.rediff.com/cgi-bin/login.cgi")
    def Handling_Action_class(self):
        self.driver.get("https://www.spicejet.com/")
        actionchains = ActionChains(self.driver)
        actionchains.move_to_element(self.driver.find_element_by_id("highlight-addons")).perform()
        actionchains.move_to_element(self.driver.find_element_by_xpath("//*[@id=\"header-addons\"]/ul/li[2]/a")).click()
    def teardown(self):
        self.driver.close()
hps = HandlingActionClass()
hps.Handling_Action_class()
hps.teardown()