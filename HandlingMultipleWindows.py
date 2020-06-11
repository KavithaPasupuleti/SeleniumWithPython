from selenium import webdriver
from selenium import webdriver
class NavigationsandScreenshot:
    def __init__(self):
        self.url1="http://www.popuptest.com/"
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")

    def Navigations_and_Screenshot(self):
        self.driver.get(self.url1)
        parentwindow=self.driver.current_window_handle
        self.driver.find_element_by_xpath("/html/body/table/tbody/tr[2]/td[2]/table/tbody/tr[2]/td[1]/font[2]/b/a").click()
        windows = self.driver.window_handles
        windows.remove(parentwindow)
        for i in windows:
            print(i)
            self.driver.switch_to_window(i)
            self.driver.close()
            print(i)
        self.driver.switch_to_window(parentwindow)
    def teardown(self):
        self.driver.close()
hps = NavigationsandScreenshot()
hps.Navigations_and_Screenshot()
hps.teardown()