# gmb_scraper.py (FINAL - With CAPTCHA/Cookie Handling and Robust Selectors)
# Scrapes Google My Business listings after clicking "More businesses".

import time
import random
import logging
import os
import pandas as pd
import gspread
import smtplib
import traceback
import re
from email.mime.text import MIMEText
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# --- These imports use your existing config files without modification ---
import config
import serp_selectors

# ==============================================================================
# --- CONFIGURATION FOR THIS SCRIPT ---
# ==============================================================================

GMB_WORKSHEET_NAME = "GMB lists"
OUTPUT_EXCEL_FILE = "GMB_Scraped_Data.xlsx"
PROGRESS_TRACKING_FILE = "gmb_completed_keywords.txt"
MAX_GMB_PAGES_TO_SCRAPE = 10

# --- CSS/XPATH SELECTORS for GMB Scraping ---
# Using a more robust XPath selector that finds the link by its visible text.
MORE_BUSINESSES_BUTTON_XPATH = "//a[contains(., 'More businesses')]"
GMB_LISTING_CONTAINER = "div.rllt__details"
GMB_NEXT_PAGE_BUTTON = '#pnnext'

DELAY_CONFIG = {
    "after_page_load": {"min": 2.5, "max": 4.0},
    "gmb_list_read": {"min": 3.0, "max": 5.0},
    "before_next_page": {"min": 2.0, "max": 3.5},
    "between_keywords": {"min": 10.0, "max": 25.0},
    "scroll_pause": {"min": 0.8, "max": 1.5}
}

# ==============================================================================
# --- LOGGING, EMAIL, AND PROGRESS TRACKING ---
# ==============================================================================

log_file_path = os.path.join(config.PROJECT_ROOT, 'gmb_scraper.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file_path, mode='w'), logging.StreamHandler()])

def send_error_email(subject, body):
    if not config.ENABLE_EMAIL_NOTIFICATIONS: return
    recipients = config.RECIPIENT_EMAIL
    logging.info(f"Preparing to send error email to: {', '.join(recipients)}")
    try:
        msg = MIMEText(body, 'plain')
        msg['Subject'], msg['From'], msg['To'] = subject, config.SENDER_EMAIL, ", ".join(recipients)
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.sendmail(config.SENDER_EMAIL, recipients, msg.as_string())
            logging.info("Error email sent successfully.")
    except Exception as e:
        logging.error(f"CRITICAL: FAILED TO SEND ERROR EMAIL. Error: {e}")

def load_completed_keywords():
    completed = set()
    progress_file_path = os.path.join(config.PROJECT_ROOT, PROGRESS_TRACKING_FILE)
    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r') as f:
            completed = set(line.strip() for line in f)
    logging.info(f"Loaded {len(completed)} completed keywords from progress file.")
    return completed

def save_completed_keyword(keyword):
    progress_file_path = os.path.join(config.PROJECT_ROOT, PROGRESS_TRACKING_FILE)
    with open(progress_file_path, 'a') as f:
        f.write(keyword + '\n')
    logging.info(f"Saved '{keyword}' to progress file.")

# ==============================================================================
# --- NEW: CAPTCHA AND COOKIE HANDLING FUNCTIONS ---
# ==============================================================================

def handle_captcha(driver, keyword):
    alert_sent = False
    start_time = time.time()
    logging.warning("!!! CAPTCHA DETECTED !!! Pausing script and waiting for manual intervention.")
    
    while time.time() - start_time < config.CAPTCHA_WAIT_TIMEOUT:
        if not driver.find_elements(By.CSS_SELECTOR, 'iframe[title="reCAPTCHA"]'):
            logging.info("CAPTCHA solved! Resuming script.")
            return True

        if not alert_sent:
            print("\n" + "="*60)
            print(f"ACTION REQUIRED: Please solve the CAPTCHA in the browser.")
            print(f"The script will wait for up to {config.CAPTCHA_WAIT_TIMEOUT / 60:.0f} minutes.")
            print("="*60 + "\n")
            
            email_subject = "GMB Scraper Alert: CAPTCHA - Action Required"
            email_body = f"Hello,\n\nThe GMB Scraper has encountered a Google CAPTCHA and is paused.\n\nKeyword: \"{keyword}\"\n\nPlease solve it in the browser. The script will resume automatically."
            send_error_email(email_subject, email_body)
            alert_sent = True
        
        time.sleep(config.CAPTCHA_CHECK_INTERVAL)
        print(".", end="", flush=True)

    logging.error(f"CAPTCHA Timeout! Aborting keyword '{keyword}'.")
    return False

def handle_cookie_consent(driver):
    """Looks for common cookie consent buttons and clicks one if found."""
    time.sleep(2) # Wait for banner to appear
    try:
        # Tries to find buttons by their text content using XPath
        consent_buttons = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Reject all')]",
            "//button[contains(., 'I agree')]"
        ]
        for xpath in consent_buttons:
            try:
                button = driver.find_element(By.XPATH, xpath)
                logging.info(f"Found a cookie consent button with text: '{button.text}'. Clicking it.")
                button.click()
                time.sleep(2) # Wait for banner to disappear
                return # Exit after clicking one
            except NoSuchElementException:
                continue # Try next button
    except Exception as e:
        logging.warning(f"Could not handle cookie consent banner: {e}")

# ==============================================================================
# --- SELENIUM & HELPER FUNCTIONS ---
# ==============================================================================

def get_humanlike_driver():
    logging.info("Initializing human-like Chrome WebDriver...")
    options = Options()
    options.add_argument(f'user-agent={random.choice(config.USER_AGENTS)}')
    options.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
    options.add_argument("--no-first-run"); options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions"); options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver

def connect_to_google_sheets():
    logging.info("Connecting to Google Sheets API...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(config.GCP_CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds); logging.info("Successfully connected to Google Sheets API.")
    return client

def get_keywords_from_sheet(worksheet):
    logging.info(f"Fetching keywords from worksheet: '{worksheet.title}'")
    keywords = [kw.strip() for kw in worksheet.col_values(1)[1:] if kw.strip()]
    logging.info(f"Successfully fetched {len(keywords)} keywords.")
    return keywords

def find_and_type_in_search_box(driver, text):
    try:
        search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[name='q']")))
        search_box.clear()
        for char in text:
            search_box.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
        search_box.send_keys(Keys.RETURN)
        return True
    except TimeoutException:
        logging.error("Could not find the search box.")
        return False

def scroll_page_down(driver):
    logging.info("Scrolling page to load all results...")
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(DELAY_CONFIG["scroll_pause"]["min"], DELAY_CONFIG["scroll_pause"]["max"]))
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: break
            last_height = new_height
        logging.info("Finished scrolling.")
    except Exception as e:
        logging.warning(f"Could not scroll the page: {e}")

# ==============================================================================
# --- CORE GMB PARSING FUNCTION (No changes needed here) ---
# ==============================================================================
def parse_gmb_listing(element, keyword):
    data = {"Keyword": keyword, "Name": None, "Rating": None, "Number of Reviews": None, "Category": None, "Years in Business": None, "Address": None, "Phone Number": None}
    try: data["Name"] = element.find_element(By.CSS_SELECTOR, "div.dbg0pd span").text
    except: pass
    try:
        rating_line = element.find_element(By.CSS_SELECTOR, "span.Y0A0hc").text
        parts = rating_line.split('·')
        if r_match := re.search(r'(\d\.\d)', parts[0]): data["Rating"] = float(r_match.group(1))
        if rev_match := re.search(r'\((\d{1,3}(,\d{3})*|\d+)\)', parts[0]): data["Number of Reviews"] = int(rev_match.group(1).replace(',', ''))
        if len(parts) > 1: data["Category"] = parts[1].strip()
    except: pass
    try:
        details_divs = element.find_elements(By.XPATH, "./div")
        full_text = " ".join([div.text for div in details_divs if div.text])
        if y_match := re.search(r'(\d+\+?)\+?\s+years in business', full_text, re.IGNORECASE): data["Years in Business"] = y_match.group(1)
        if p_match := re.search(r'(\d{5}\s\d{5}|\d{10}|[0-9\s]{8,})', full_text):
            phone = re.sub(r'\s+', '', p_match.group(0)).strip()
            if len(phone) >= 8: data["Phone Number"] = p_match.group(0).strip()
        for div in details_divs:
            text = div.text.strip()
            if not text or "years in business" in text.lower() or (data["Phone Number"] and data["Phone Number"] in text) or "·" in text or "Open" in text or "Closes" in text or "On-site services" in text: continue
            if len(text) > 15:
                data["Address"] = text
                break
    except: pass
    return data

# ==============================================================================
# --- MAIN EXECUTION BLOCK (Updated Logic) ---
# ==============================================================================
if __name__ == "__main__":
    logging.info(f"--- Starting GMB Scraper Script for worksheet '{GMB_WORKSHEET_NAME}' ---")
    driver = None
    all_gmb_data = []
    try:
        completed_keywords = load_completed_keywords()
        gspread_client = connect_to_google_sheets()
        worksheet = gspread_client.open(config.SHEET_NAME).worksheet(GMB_WORKSHEET_NAME)
        keywords_to_scrape = get_keywords_from_sheet(worksheet)
        driver = get_humanlike_driver()
        
        for i, keyword in enumerate(keywords_to_scrape):
            if keyword in completed_keywords:
                logging.info(f"Skipping already completed keyword: '{keyword}'")
                continue

            logging.info(f"\n--- Processing keyword {i+1}/{len(keywords_to_scrape)}: '{keyword}' ---")
            driver.get(config.SEARCH_URL)
            handle_cookie_consent(driver) # Handle cookies first
            time.sleep(random.uniform(DELAY_CONFIG["after_page_load"]["min"], DELAY_CONFIG["after_page_load"]["max"]))
            
            if not find_and_type_in_search_box(driver, keyword): continue
            
            # --- NEW: Check for CAPTCHA right after search ---
            if driver.find_elements(By.CSS_SELECTOR, 'iframe[title="reCAPTCHA"]'):
                if not handle_captcha(driver, keyword):
                    save_completed_keyword(keyword) # Mark as failed/skipped
                    continue # Move to next keyword
            
            try:
                logging.info("Looking for 'More businesses' button...")
                more_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, MORE_BUSINESSES_BUTTON_XPATH)))
                driver.execute_script("arguments[0].click();", more_button)
                logging.info("Clicked 'More businesses'.")
            except TimeoutException:
                logging.warning(f"Could not find 'More businesses' button for '{keyword}'. Skipping.")
                save_completed_keyword(keyword)
                continue

            for page_num in range(1, MAX_GMB_PAGES_TO_SCRAPE + 1):
                logging.info(f"--- Scraping GMB Page {page_num} for '{keyword}' ---")
                time.sleep(random.uniform(DELAY_CONFIG["gmb_list_read"]["min"], DELAY_CONFIG["gmb_list_read"]["max"]))
                scroll_page_down(driver)
                listings = driver.find_elements(By.CSS_SELECTOR, GMB_LISTING_CONTAINER)
                logging.info(f"Found {len(listings)} listings on this page.")
                if not listings: break
                for listing_element in listings:
                    if parsed_data := parse_gmb_listing(listing_element, keyword):
                        if parsed_data.get("Name"): all_gmb_data.append(parsed_data)
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, GMB_NEXT_PAGE_BUTTON)
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(random.uniform(DELAY_CONFIG["before_next_page"]["min"], DELAY_CONFIG["before_next_page"]["max"]))
                except NoSuchElementException:
                    logging.info("No 'Next' button found. End of results.")
                    break
            
            save_completed_keyword(keyword)
            logging.info(f"Finished scraping for '{keyword}'. Taking a break...")
            time.sleep(random.uniform(DELAY_CONFIG["between_keywords"]["min"], DELAY_CONFIG["between_keywords"]["max"]))

        if all_gmb_data:
            output_path = os.path.join(config.PROJECT_ROOT, OUTPUT_EXCEL_FILE)
            logging.info(f"Scraping complete. Saving {len(all_gmb_data)} listings to {output_path}...")
            pd.DataFrame(all_gmb_data).to_excel(output_path, index=False)
            logging.info("Successfully saved data to Excel.")
        else:
            logging.warning("Scraping finished, but no new data was collected.")
    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.critical(f"A critical, unhandled error occurred: {e}\n{error_traceback}")
        send_error_email("GMB Scraper Alert: SCRIPT CRASHED", f"The GMB Scraper script has crashed.\n\nError:\n{e}\n\nTraceback:\n{error_traceback}")
    finally:
        if driver:
            logging.info("Closing WebDriver.")
            driver.quit()
        logging.info("--- GMB Scraper Script Finished ---")