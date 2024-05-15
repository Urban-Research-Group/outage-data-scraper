## Code Optimization

Based on one outage map **AEP Texas, Inc.**

Time (Original): 27.89s

### **Optimize Selenium WebDriver Use**

1. **Disable Images and CSS**

   Loading images and CSS files can be time-consuming.

   ```python
   chrome_options.add_argument("--blink-settings=imagesEnabled=false")
   ```

   Outcome: **27.8 -> 27.0**

2. **Page Load Strategy**

   Implement the "eager" page load strategy, instruct the browser to proceed with the script without waiting for all resources to finish loading

   ```python
   desired_capabilities["pageLoadStrategy"] = "eager"
   ```
   
   Outcome: **27.0 -> 25.5**

3. **Adjust fixed timeouts:** 

   Use WebDriverWait to wait for a certain condition to be met before proceeding further in the script.

   ```python
   WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mapwise-web-modal-header h5")))
   ```

   This line of code instructs Selenium to wait up to 10 seconds for an `h5` element within a `div` with the specified class to appear on the page.

4. **Use Fast Selectors**

   Speed: find_element_by_id > find_element_by_css_selector > XPath

   Note: need to delve into each scraper, see the html and choose best way. 

### **Improve Network Performance**

1. **Dynamic Request** 

   Iterates over all network requests recorded by the webdriver and filters those that meet the following criteria:

   - The URL contains the specified substring `part_of_url`.
   - The request has a response (i.e., `r.response` is not None).
   - The response's content type (Content-Type) includes `application/json`.

   ```python
       def wait_for_json_request(self, part_of_url, timeout=10):
           """
           Waits for a JSON request to be made that contains a specific part in its URL.
           Args:
               part_of_url: A substring of the URL to look for.
               timeout: How long to wait for the request, in seconds.
           """
           start_time = time.time()
   
           if self.driver == None:
               self.driver = self.init_webdriver()
   
           while True:
               requests = [
                   r
                   for r in self.driver.requests
                   if part_of_url in r.url
                   and r.response
                   and "application/json" in r.response.headers.get("Content-Type", "")
               ]
               if requests:
                   return requests[0]  # Return the first matching request
               elif time.time() - start_time > timeout:
                   raise TimeoutError(
                       f"JSON request containing '{part_of_url}' not found within timeout."
                   )
               time.sleep(0.5)  # Short sleep to avoid busy loop
   ```

​	This is a good idea, but needs to be incoporated into more scraper.

2. **Concurrency**

   Make parallel request. Haven't tried.

### Parsing Logic

1. **Efficient Beautiful soup**

   lxml is faster than html.parser
   Source: https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-a-parser

   ```python
   BeautifulSoup(markup, "lxml")
   ```

### For loop 

1. List->  list comprehension

   Change from appending to a list to list comprehension will speed the list operation about 50%.

   Source: https://stackoverflow.com/questions/30245397/why-is-a-list-comprehension-so-much-faster-than-appending-to-a-list

