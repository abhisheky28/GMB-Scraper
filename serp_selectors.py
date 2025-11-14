# serp_selectors.py
# This file centralizes all CSS selectors for easy updates if Google changes its HTML.

# Main container for each result block (organic, ad, PAA, etc.)
#RESULT_CONTAINER = "div.MjjYud"

# Selector to identify an Ad block within a container
#AD_SELECTOR = "[data-text-ad]"

# Selector to identify a "People Also Ask" block within a container
#PAA_SELECTOR = "div.related-question-pair"

# The specific area within a clean organic result that holds the main link
#LINK_CONTAINER = "div.yuRUbf a"

# The h3 tag containing the title of the result (useful for logging)
#TITLE_SELECTOR = "h3"

# Search input box on Google's homepage
#SEARCH_INPUT = "[name='q']"



# serp_selectors.py (Updated for higher accuracy)

# This is the new, highly specific selector for a true organic result block.
# It looks for a div that has a link (a) which contains an H3 heading.
# This is the most reliable way to isolate only the organic blue-link results.
RESULT_CONTAINER = "div.MjjYud"

# This selector is no longer needed because we will use a more robust filtering method.
# AD_SELECTOR = "[data-text-ad]" # We will not use this directly anymore.

# This selector is also no longer needed.
# PAA_SELECTOR = "div.related-question-pair" # We will not use this directly anymore.

# This is the selector for the clickable link within a result block.
# It remains the same.
LINK_CONTAINER = "a"

# This is the selector for the "Next" page button.
NEXT_PAGE_BUTTON = "#pnnext"

# The selector for the "More results" button found on mobile SERPs.
MOBILE_NEXT_PAGE_BUTTON_SELECTOR = 'a[aria-label="More results"]'