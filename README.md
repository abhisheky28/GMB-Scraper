# GMB-Scraper
Imagine you need a list of all the gyms in New York City. Normally, you would have to search on
Google Maps, click on each business, and manually copy-paste their name, address, phone number,
and rating into a spreadsheet. This is incredibly time-consuming and prone to errors.
This tool solves that problem.
It is a smart Python script that acts like a highly efficient assistant. You give it a list of search terms
(like "gyms in New York City"), and it automatically:
1. Opens a real Chrome browser.
2. Searches Google for your term.
3. Clicks the "More businesses" button to see the full list.
4. Methodically scrapes the details of each business—Name, Rating, Number of Reviews,
Phone Number, Address, and more.
5. Saves all this valuable data into a clean, organized Excel file on your computer.
It's designed to behave like a human to avoid being blocked and even has a system to pause and
alert you if it encounters a CAPTCHA, so you can solve it and let the script continue its work.
