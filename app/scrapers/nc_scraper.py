import datetime
import time
import pandas as pd
from selenium import webdriver
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from app.scrapers.util import timenow
from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper3 as GA_Scraper3,
    Scraper6 as GA_Scraper6,
    Scraper7 as GA_Scraper7,
    Scraper9 as GA_Scraper9,
)


class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            print(key)
            if val:
                df = pd.DataFrame(val)
                df = df[(df["Number of Outages"] != 0)]
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                data.update({key: df})
            else:
                print(
                    f"no '{key}' outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )

        self.driver.close()
        self.driver.quit()

        return data

    def fetch(self):
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        try:
            span_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//span[@class='jurisdiction-selection-select-state__item-text' and text()='Duke Energy Carolinas']"))
            )
    
            # Click on the span element
            span_element.click()
            
            print("Clicked on 'Duke Energy Carolinas' successfully!")

            h3_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'maps-panel-title'))
            )
            
            # Click on the h3 element
            h3_element.click()
            
            print("Clicked on 'Report & View Outages' successfully!")

            outage_summary_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Outage Summary']"))
            )
            
            # Click on the "OUTAGE SUMMARY" button
            outage_summary_button.click()

            print("summary clicked successfully!")

            outage_summary_table_span = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//span[@class='county-panel-outage-info-summary-btn-text ng-tns-c41-6' and text()='Outage Summary Table']"))
            )

            # Click on the "Outage Summary Table" span element
            outage_summary_table_span.click()
            
            print("Clicked on 'Outage Summary Table' successfully!")

            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "outage-summary-table-content-row"))
            )

            # Find all table rows
            table_rows = self.driver.find_elements(By.CLASS_NAME, "outage-summary-table-content-row")
            
            # Initialize dictionary to hold table data
            table_data = {
                'Location': [],
                'Number of Outages': [],
                'Affected Customers': [],
                'Percentage Affected': [],
                'Last Updated': []
            }
            
            # Iterate over table rows
            for row in table_rows:
                cells = row.find_elements(By.CLASS_NAME, "outage-summary-table-content-body-item")
                table_data['Location'].append(cells[0].text)
                table_data['Number of Outages'].append(cells[1].text)
                table_data['Affected Customers'].append(cells[2].text)
                table_data['Percentage Affected'].append(cells[3].text)
                table_data['Last Updated'].append(cells[4].text)

            print("Table data created successfully!")
        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

        return table_data

class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            print(key)
            if val:
                df = pd.DataFrame(val)
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                data.update({key: df})
            else:
                print(
                    f"no '{key}' outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )

        self.driver.close()
        self.driver.quit()

        return data
    
    def fetch(self):
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        try:
            menu_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'btn-menu'))
            )
    
            # Click on the menu button
            menu_button.click()

            print("clicked menu!")

            summary_icon = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'summary-icon'))
            )
            
            # Click on the summary icon
            summary_icon.click()



            # Click on the arrow to expand the menu
            
            # Wait for the "Area / County" link to be clickable
            area_county_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'view-summary-county-muni'))
            )
            
            # Click on the "Area / County" link
            area_county_link.click()

            print("clicked summary!")

            nc_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//span[text()="North Carolina"]'))
            )
            
            # Click on the expand/collapse arrow for North Carolina
            arrow_element = nc_element.find_element(By.XPATH, './preceding-sibling::span[@class="treegrid-expander treegrid-expander-collapsed"]')
            arrow_element.click()

            print("clicked nc!")

            rows = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.treegrid-parent-1.level2'))
            )
            
            # Create a dictionary to store the data
            data = {}

            # Loop through each row and extract the data
            for row in rows:
                area_name = row.find_element(By.CSS_SELECTOR, '.area_nameCol span:nth-child(3)').text        
                cust_a = row.find_element(By.CSS_SELECTOR, '.cust_aCol span').text
                cust_s = row.find_element(By.CSS_SELECTOR, '.cust_sCol span').text
                if (cust_a != '0'):
                    data[area_name] = {'cust_a': cust_a, 'cust_s': cust_s}
            
        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

        return data


class NCScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper7)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper6)
        elif layout_id == 5:
            obj = super().__new__(Scraper6)
        elif layout_id == 6:
            obj = super().__new__(Scraper7)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj