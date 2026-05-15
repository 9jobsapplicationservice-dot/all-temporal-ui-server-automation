import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def test_chrome():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    print("Attempting to open Chrome...")
    try:
        driver = webdriver.Chrome(options=options)
        print("Chrome opened successfully!")
        driver.get("https://www.google.com")
        print(f"Title: {driver.title}")
        driver.quit()
        return True
    except Exception as e:
        print(f"Failed to open Chrome: {e}")
        return False

if __name__ == "__main__":
    test_chrome()
