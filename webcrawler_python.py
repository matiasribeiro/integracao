from selenium import webdriver
from xvfbwrapper import Xvfb
import time

display = Xvfb() # gerar um ambiente virtual
display.start()

driver = webdriver.Firefox()
html = driver.get('https://tramita.tce.pb.gov.br/tramita/consultatramitacao?documento=21385_18')
time.sleep(6) # tempo para carregar os dados na tela

driver.switch_to.frame(driver.find_elements_by_tag_name("iframe")[-1])

print(driver.find_element_by_id("body:mainForm:numeroProcessoAdministrativo").get_attribute('innerHTML'))

driver.quit()
display.stop()

