# coding: utf8

import re
import time
import datetime
from resources import Settings
from selenium import webdriver as webdriver


def get_browser():
    while True:
        try:
            #browser = webdriver.Firefox()
            browser = webdriver.Chrome()
        except:
            time.sleep(120)
            continue
        else:
            browser.implicitly_wait(10)
            return browser

def open_onyma():
    browser = get_browser()
    while True:
        try:
            browser.get("https://10.144.196.37/onyma/")
            element = browser.find_element_by_id("LOGIN")
            element.send_keys(Settings.onyma_login)
            element = browser.find_element_by_id("PASSWD")
            element.send_keys(Settings.onyma_password)
            element = browser.find_element_by_id("enter")
            element.click()
        except:
            browser.quit()
            time.sleep(15)
            browser = get_browser()
        else:
            break
    return browser

def count_sessions(bill,  dmid,  tmid,  date,  browser,  cursor):
    try:
        browser.get("https://10.144.196.37/onyma/main/ddstat.htms?bill={}&dt={}&mon={}&year={}&service=201&dmid={}&tmid={}".format(bill,  date.day,  date.month, date.year,  dmid,  tmid))
        browser.find_element_by_id("onymaPageFooter")
    except:
        return -1
    else:
        if re.search(r'<td class="foot">Все</td><td class="pgout" colspan="5">.+?<b>(\d+)</b>',  browser.page_source.__repr__()):
            count = re.search(r'<td class="foot">Все</td><td class="pgout" colspan="5">.+?<b>(\d+)</b>',  browser.page_source.__repr__()).group(1)
        else:
            print('count_sessions: bill {},  dmid {}, tmid {}, date {}',format(bill,  dmid,  tmid,  date))
            return False
        return int(count)

def find_account_param(browser, account_name):
    try:
        browser.get("https://10.144.196.37/onyma/main/dogsearch.htms?menuitem=1851")
        element = browser.find_element_by_id("sitename")
        element.send_keys(account_name)
        element = browser.find_element_by_id("search")
        element.click()
        element = browser.find_element_by_partial_link_text("Договор")
        element.click()                
        element = browser.find_element_by_id("menu4185")
        element.click()
        elements = browser.find_elements_by_partial_link_text(datetime.date.today().strftime('-%Y'))
        elements_month = [x.get_attribute('href') for x in elements if 'menuitem=1844' in x.get_attribute('href')]
        elements = browser.find_elements_by_partial_link_text((datetime.date.today() - datetime.timedelta(days=365)).strftime('-%Y'))
        elements_month += [x.get_attribute('href') for x in elements if 'menuitem=1844' in x.get_attribute('href')]

        max_date = datetime.datetime(2000, 1, 1)
        url = ''
        
        for element_month in elements_month:
            browser.get(element_month)    
            elements = browser.find_elements_by_link_text(account_name)
            elements_service = [x.get_attribute('href') for x in elements if 'service=201' in x.get_attribute('href')]    
    
            for element_service in elements_service:
                browser.get(element_service)
                url_date = browser.current_url
                year = re.search(r'year=(.+?)&', url_date).group(1)
                month = re.search(r'mon=(.+?)&', url_date).group(1)
                elements_date = browser.find_elements_by_partial_link_text('.{}.{}'.format(month, year[2:]))
                for element_date in elements_date:
                    if 'menuitem=4185' in element_date.get_attribute('href'):
                        current_date = datetime.datetime.strptime(element_date.get_attribute('title'), '%d.%m.%y')
                        if current_date >= max_date:
                            max_date = current_date
                            url = browser.current_url
                            break
                        else:
                            break
            if url != '':
                break

        if url != '':
            bill = re.search(r'bill=(\d+)', url).group(1)
            dmid = re.search(r'dmid=(\d+)', url).group(1)
            tmid = re.search(r'tmid=(\d+)', url).group(1)
            return bill, dmid, tmid
        else:
            print('Не удалось обработать учетное имя: {}'.format(account_name))
            return False        
    except:
        return -1