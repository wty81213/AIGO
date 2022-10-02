from calendar import month
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import os
 
options = Options()
# options.add_argument("--disable-notifications")
p = {"download.default_directory": "D:\ML\waitingTime\data\day"}
options.add_experimental_option("prefs", p)
driver = webdriver.Chrome('./chromedriver', options=options)

weather_month_page = 'https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station={}&stname=%25E7%25AB%25B9%25E5%25AD%2590%25E6%25B9%2596&datepicker={}-{}&altitude=607.1m'
weather_day_page = "https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station={}&stname=%25E9%259E%258D%25E9%2583%25A8&datepicker={}-{}-{}&altitude=837.6m"

# location = ["466930_竹子湖", "466910_鞍部", "466920_臺北", "C0A980_社子", "C0A770_科教館", 
#            "C0A9C0_天母", "C0A9F0_內湖", "C0AH40_平等", "C0AH70_松山", "C0AI40_石牌", "C0AC40_大屯山", 
#            "C0AC70_信義", "C0AC80_文山", "C1AC50_關渡", "C1AI50_國三N016K", "C1A730_公館 (撤銷站)", 
#            "C0A9G0_南港 (撤銷站)", "C0A9E0_士林 (撤銷站)", "C0A990_大崙尾山 (撤銷站)", 
#            "C0A9A0_大直 (撤銷站)", "C0A9B0_石牌 (撤銷站)", "466921_臺北(師院) (撤銷站)"]

# location = ["466930", "466910", "466920", "C0A980", "C0A770", 
#           "C0A9C0", "C0A9F0", "C0AH40", "C0AH70", "C0AI40", "C0AC40", 
#           "C0AC70", "C0AC80", "C1AC50", "C1AI50", "C1A730", 
#           "C0A9G0", "C0A9E0", "C0A990", "C0A9A0", "C0A9B0", "466921"]

location = ["466920"]

year = '2017'

months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
days_28 = [ str(i + 1).zfill(2) for i in range(28) ]
days_30 = [ str(i + 1).zfill(2) for i in range(30) ] 
days_31 = [ str(i + 1).zfill(2) for i in range(31) ] 

for i in range(len(location)):
  for j in range(len(months)):
    if months[j] in ["01", "03", "05", "07", "08", "10", "12"]:
      days = days_31
    elif months[j] == "02":
      days = days_28
    else:
      days = days_30

    for k in range(len(days)):
        
        # driver.get(weather_month_page.format(location[i], year, months[j]))
        driver.get(weather_day_page.format(location[i], year, months[j], days[k]))
        download_csv_button_xpath = "//*[@id='downloadCSV']"
        download_csv_button = driver.find_element(by=By.XPATH, value=download_csv_button_xpath)
        download_csv_button.click()
        time.sleep(0.08)


driver.quit()