## Code Optimization



1. AEP Texas, Inc.

​	Time(right now): 27.8976s

Approch:

### **Optimize Selenium WebDriver Use**

1. **Disable Images and CSS**

   Loading images and CSS files can be time-consuming.

   ![image-20240220191336816](/Users/xuanzhangliu/Library/Application Support/typora-user-images/image-20240220191336816.png)

   **27.8 -> 27.0**

2. **Page Load Strategy**

   Implement the "eager" page load strategy, instruct the browser to proceed with the script without waiting for all resources to finish loading

   ![image-20240220191917817](/Users/xuanzhangliu/Library/Application Support/typora-user-images/image-20240220191917817.png)

​	**27.0 -> 25.5**

3. **Use Fast Selectors**

   Speed: find_element_by_id > find_element_by_css_selector > XPath

   Note: need to delve into each scraper, see the html and choose best way. 

```python
class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        suffix = ["?report=report-panel-county", "?report=report-panel-zip"]

        data = {}
        for s in suffix:
            url = self.url + s
            print(f"fetching {self.emc} outages from {url}")
            find_type = "class"
            findkeyword = "report-table-tree"
            html = self.get_page_source(url, find_type, findkeyword)
            # print(html)

            # parse table
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", attrs={find_type: findkeyword})
            rows = table.find_all("tr")

            data_rows = rows[2:]
            raw_data = []
            for row in data_rows:
                cells = row.find_all("td")
                raw_data.append([cell.text.strip() for cell in cells])

            loc = "COUNTY" if s == "?report=report-panel-county" else "ZIP"
            header = ["VIEW", loc, "CUSTOMER OUTAGES", "CUSTOMERS SERVED", "% AFFECTED"]
            table_data = [dict(zip(header, row)) for row in raw_data]
            df = pd.DataFrame(table_data)[
                [loc, "CUSTOMER OUTAGES", "CUSTOMERS SERVED", "% AFFECTED"]
            ]
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            df = df[df["CUSTOMER OUTAGES"] != "0"]
            key = "per_county" if loc == "COUNTY" else "per_zipcode"
            data.update({key: df})

        return data


    def get_page_source(self, url=None, find_type = None, findkeyword = None, timeout=10):
        """Get the page source of the url, waiting for a specific element type to be present."""
        url = url if url else self.url
        self.driver.get(url)
        
        # Map find_type to Selenium's By types
        find_type_map = {
            "class": By.CLASS_NAME,
            "id": By.ID,
            "tag": By.TAG_NAME,
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "name": By.NAME,
            "link_text": By.LINK_TEXT,
            "partial_link_text": By.PARTIAL_LINK_TEXT,
        }
        
        # Check if the provided find_type is supported
        if find_type not in find_type_map:
            raise ValueError(f"Unsupported find_type: {find_type}")
        by_type = find_type_map[find_type]
            # let the page load
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by_type, findkeyword))
            )
        except TimeoutException:
            print(f"Timed out waiting for page to load: {url}")
            return ""  # Return empty string or handle as appropriate
        
        page_source = self.driver.page_source
        return page_source
```

### **Improve Network Performance**

1. Adjust timeouts: 

   Reduce the timeout parameter.

   timeout: 5->1. Still working. And time reduced significantly.

   **25.5->9.8**

   But not secure.

2. **Concurrency**

   Make parallel request. 

### Parsing Logic

1. **Efficient Beautiful soup**

   lxml is faster than html.parser
   from https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-a-parser

   ```python
   BeautifulSoup(markup, "lxml")
   ```

### For loop 

Change from appending to a list to list comprehension will speed the list operation about 50%.

https://stackoverflow.com/questions/30245397/why-is-a-list-comprehension-so-much-faster-than-appending-to-a-list

**9.8->9.3**

