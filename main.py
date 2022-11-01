# coding=UTF-8
import schedule
from pandas import DataFrame
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait

# 设置浏览器驱动
# chrome_options 初始化选项
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('useAutomationExtension', False)
# chrome_options.add_argument('--disable-gpu')  # 禁用gpu加速
chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
# 关闭自动测试状态显示 // 会导致浏览器报：请停用开发者模式
# window.navigator.webdriver还是返回True,当返回undefined时应该才可行。
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

def job_1():
    df = {'name': [], 'out': [], 'served': [], 'impact': []}
    driver = webdriver.Chrome(service =Service("./chromedriver.exe"),options=chrome_options)
    driver.get('https://www.outageentry.com/Outages/outage.php?Client=walton')
    # 设置等待
    wait = WebDriverWait(driver, 60, 0.5)
    # 使用匿名函数
    wait.until(lambda diver: driver.find_element(By.XPATH,
                                                 '//div[@class="px-3 dv-selector button_box light_back button_box-right"]'))
    list = driver.find_element(By.XPATH, '//div[@class="px-3 dv-selector button_box light_back button_box-right"]')
    list.click()
    time.sleep(1)
    tmp = [i.text for i in driver.find_elements(By.XPATH, '//table[@class="mt-4"]//tbody//tr/td[1]')]
    df['name'] = [i for i in tmp if i != ""]
    df['out'] = [i.text.split('of')[0].strip() for i in
                 driver.find_elements(By.XPATH, '//table[@class="mt-4"]//tbody//tr/td[2]')]
    df['served'] = [i.text.split('of')[-1].strip() for i in
                    driver.find_elements(By.XPATH, '//table[@class="mt-4"]//tbody//tr/td[2]')]
    df['impact'] = [i.text for i in driver.find_elements(By.XPATH, '//table[@class="mt-4"]//tbody//tr/td[3]')]

    df = DataFrame(df)
    try:
        df.to_excel('outageentry.xlsx', header=True, index=False)
    except:
        print('请检查outageentry.xlsx当前是否被占用')
    print('outageentry.xlsx已完成 当前时间' + time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
    driver.quit()


def job_2():
    df = {'county': [], 'out': [], 'served': [], '%out': []}
    driver = webdriver.Chrome(service =Service("./chromedriver.exe"),options=chrome_options)
    driver.get('https://webapps.jacksonemc.com/nisc/maps/MemberOutageMap/')
    # 设置等待
    wait = WebDriverWait(driver, 60, 0.5)
    # 使用匿名函数
    wait.until(lambda diver: driver.find_elements(By.XPATH, '//td[contains(@class,"summary-region-column")]'))

    df['county'] = [i.text for i in driver.find_elements(By.XPATH, '//td[contains(@class,"summary-region-column")]')]
    df['out'] = [i.text for i in driver.find_elements(By.XPATH, '//td[contains(@class,"summary-number-out-column")]')]
    df['served'] = [i.text for i in
                    driver.find_elements(By.XPATH, '//td[contains(@class,"summary-number-served-column")][1]')]
    df['%out'] = [i.text for i in
                  driver.find_elements(By.XPATH, '//td[contains(@class,"summary-number-served-column")][2]')]
    df = DataFrame(df)
    try:
        df.to_excel('jacksonemc.xlsx', header=True, index=False)
    except:
        print('请检查jacksonemc.xlsx当前是否被占用')
    print('jacksonemc.xlsx已完成 当前时间' + time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
    driver.quit()


if __name__ == '__main__':
    planTime = int(input('请输入定时时间（分钟）:'))
    schedule.every(planTime).minutes.do(job_1)
    schedule.every(planTime).minutes.do(job_2)
    while True:
        schedule.run_pending()
        time.sleep(30)
