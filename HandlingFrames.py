from selenium import webdriver
class HandlingFrames:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")
        self.driver.get("https://www.selenium.dev/selenium/docs/api/java/")
    def Handling_Frames(self):
        sample =self.driver.find_elements_by_tag_name("frame")
        self.driver.switch_to_frame(1)
        self.driver.switch_to_default_content()
        self.driver.switch_to_frame("classFrame")
        self.driver.switch_to_default_content()
        for i in sample:
            i.click()
            self.driver.switch_to_default_content()
    def teardown(self):
        self.driver.close()
hps = HandlingFrames()
hps.Handling_Frames()
hps.teardown()