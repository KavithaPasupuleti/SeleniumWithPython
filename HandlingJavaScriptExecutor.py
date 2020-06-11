from selenium import webdriver
class HandlingJavaScriptExecutor:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="/home/easyway/Desktop/selnium jars/chromedriver_linux64/chromedriver")
        self.driver.get("https://mail.rediff.com/cgi-bin/login.cgi")
    def Handling_javascript_executor(self):
        title =self.driver.execute_script("return document.title")
        assert title== "Rediffmail" ,  "Title is not as Expected"
        print(self.driver.execute_script("return document.URL"))
        self.driver.execute_script("document.getElementById('login1').value='sampleid'")
        self.driver.execute_script("arguments[0].click()",self.driver.find_element_by_name("proceed"))
        alrt = self.driver.switch_to_alert()
        #assert alrt.text=="Please enter a valid user name", "Alert Message is not as expected"
        alrt.accept()
        self.driver.execute_script("arguments[0].style.border='6px solid red'",
                                   self.driver.find_element_by_name("proceed"));
    def teardown(self):
        self.driver.close()
hps = HandlingJavaScriptExecutor()
hps.Handling_javascript_executor()
hps.teardown()