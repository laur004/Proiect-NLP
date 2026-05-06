from playwright.sync_api import sync_playwright
import time
import re
import os


# PRE-PROCESSING INFO
INPUT_FILE = "insta_video_urls.txt"
MAX_COMMENTS_PER_VIDEO = 300
OUTPUT_FILE = "out.txt"

# LOGIN INFO
LOGIN_PAGE = "https://www.instagram.com/accounts/login/"
USERNAME = "scraperinoo"
PASSWORD = "" # password hidden for upload

# IN-PAGE LOCATORS
COMMENTS_BUTTON_SELECTOR = 'svg[aria-label="Comment"]'
COMMENT_CONTAINER_SELECTOR = 'span[dir="auto"][style*="18px"]'

STUCK_ADMINTIME = 15 # seconds
WAITTIME_CHUNK = int(STUCK_ADMINTIME / 5)
# OK
def parse_urls_from_file(filepath):
    """Reads the text file and pairs URLs with their categories."""
    tasks = []
    current_category = "unknown"

    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return tasks

    with open(filepath, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    for line in lines:
        if "instagram.com" in line:
            tasks.append({"url": line, "category": current_category})
        else:
            current_category = line

    return tasks


# OK
def login(page):
    """
    Handles the initial login steps.
    """
    print("[...] Checking for cookie popup.")
    try:

        page.get_by_role("button", name="Decline optional cookies").click(timeout=3121)
        print("Successfully declined optional cookies.")
        time.sleep(2)

    except Exception as e:
        print("  [?] Cookie popup not found or already dismissed.\n[...] Continuing.")

    print("[...] Entering credentials.")
    try:
        # Target the input fields using their name attributes
        email_input = page.locator('input[name="email"]')
        pass_input = page.locator('input[name="pass"]')

        # Wait for the email field to appear, then fill them

        email_input.wait_for(timeout=5000)
        email_input.fill(USERNAME)
        pass_input.fill(PASSWORD)

        # Press Enter to submit the form, bypassing the need to click the messy button div
        pass_input.press("Enter")

        print("[...] Submitted login. Waiting for page load.")

        # Wait for the network to settle down after logging in
        page.wait_for_load_state('networkidle')
        time.sleep(4)

    except Exception as e:
        print("  [?] Couldn't find email and password input. User (may be) already logged in.\n[...] Continuing.")


# OK
def open_comments(page):
    """
    Finds the comment icon and clicks it to open the side panel.
    """
    print("[...] Opening comment section.")
    try:
        # Target the SVG icon directly using its aria-label
        comment_icon = page.locator(COMMENTS_BUTTON_SELECTOR).first

        # Wait for the icon to actually appear on screen
        comment_icon.wait_for(state="visible", timeout=4980)

        comment_icon.click(force=True)

        print(" [:D] Successfully clicked the comment button!")

        # Give the comment panel a second to physically slide out/render
        time.sleep(2)
        return True

    except Exception as e:
        print(f"[!!!] Failed to find or click the comment button: {e}")
        return False


# OK
def extract_visible_comments(page, batch_size=67):
    """
    Extracts text while filtering out usernames and timestamps.
    Only processes the last N elements to prevent RPC lag as the DOM grows.
    """
    comments = []
    try:
        # Target spans with the 18px line-height
        all_elements = page.locator('span[dir="auto"][style*="18px"]').all()

        # Slice to grab only the most recent chunk (e.g., last 100)
        # Playwright gets the list of handles quickly; the lag comes from interacting with them.
        elements_to_process = all_elements[-batch_size:]

        for el in elements_to_process:
            full_text = el.inner_text().strip()

            # Skip obvious timestamps (e.g., '1w', '24h', '10m')
            if re.match(r'^\d+[wdhm]$', full_text):
                continue

            # Filter Names/Metadata
            links = el.locator('a').all()
            is_metadata = False

            for link in links:
                link_text = link.inner_text().strip()
                if link_text == full_text:
                    is_metadata = True
                    break

            if is_metadata:
                continue

            # Final Check
            if full_text:
                clean_text = full_text.replace('\n', ' ').replace('\r', '')
                comments.append(clean_text)

        # Remove duplicates within the current visible batch
        return list(set(comments))

    except Exception as e:
        print(f"  [!!!] Error extracting comments: {e}")
        return []


# OK
def load_more_comments(page):
    try:
        # We inject JS to find the container dynamically based on its actual CSS behavior
        # rather than relying on randomized Instagram class names.
        scrolled = page.evaluate("""() => {
            // Instagram usually houses comments in a dialog or a main article container
            const wrappers = document.querySelectorAll('[role="dialog"], article');

            for (const wrapper of wrappers) {
                // Hunt for the inner div that actually handles the scrolling
                const scrollable = Array.from(wrapper.querySelectorAll('div')).find(el => {
                    const style = window.getComputedStyle(el);
                    return (style.overflowY === 'auto' || style.overflowY === 'scroll') 
                           && el.scrollHeight > el.clientHeight;
                });

                if (scrollable) {
                    // Scroll down a ridiculous set amount
                    scrollable.scrollBy(0, 9999999);

                    // Force a synthetic scroll event. 
                    // Instagram's React UI needs this to trigger the lazy-load network request.
                    scrollable.dispatchEvent(new Event('scroll'));
                    return true;
                }
            }
            return false;
        }""")

        if scrolled:
            time.sleep(2)  # Give the network time to fetch the next batch
            return True

        print("  [?] Internal scrollbar container not found. Comments might be fully loaded or UI changed.")
        time.sleep(0.5)
        return True

    except Exception as e:
        print(f"   [!!!] Scroll failed: {e}")
        return False


# OK
def process_video(page, url, category, out_file):
    """Handles the full scraping loop for a single video."""
    print(f"[...] Opening <{category}> reel {url}")
    page.goto(url)

    # Wait for the page to initially load
    time.sleep(5)

    collected_comments = set()
    scroll_attempts = 0
    max_scrolls = 5000  # Failsafe so it doesn't get stuck in an infinite loop
    open_comments(page)
    stuck_index = 0
    previous_length = 0

    while len(collected_comments) < MAX_COMMENTS_PER_VIDEO and scroll_attempts < max_scrolls:
        # Extract what is currently on screen
        current_batch = extract_visible_comments(page)
        collected_comments.update(current_batch)

        print(f"  [*] Progress: {len(collected_comments)}/{MAX_COMMENTS_PER_VIDEO} comments")

        if len(collected_comments) >= MAX_COMMENTS_PER_VIDEO:
            break

        # stuck detection: every 10 repeated loops wait for admin to fix the issue
        if len(collected_comments) == previous_length:
            stuck_index += 1
        else:
            stuck_index = 0
        if (stuck_index + 1) % 10 == 0:
            print(f" [!?] scrapping stuck -> comment section not loading!\n[...] waiting for admin actions.")
            for i in range (0, STUCK_ADMINTIME, WAITTIME_CHUNK):
                print(f"      time remaining: {STUCK_ADMINTIME-i}s")
                time.sleep(WAITTIME_CHUNK)
        previous_length = len(collected_comments)

        # Try to scroll and load more
        scrolled = load_more_comments(page)

        # Fallback: scroll down if there's no button but we need to trigger lazy loading
        if not scrolled:
            page.mouse.wheel(0, 1000)
            time.sleep(1)

        scroll_attempts += 1

    # Write the results to the file
    written = 0
    for comment in list(collected_comments)[:MAX_COMMENTS_PER_VIDEO]:
        # Format: <comment_text> 3 <category> \n
        out_file.write(f"{comment} INSTAGRAM {category}\n")
        written += 1

    print(f" [:D] Done! Wrote {written} comments for this video!")


def main():
    # LOAD URLS
    tasks = parse_urls_from_file(INPUT_FILE)
    if not tasks:
        return
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="insta_session",
            headless=False,
            slow_mo=50
        )

        page = context.pages[0]

        # LOGIN
        page.goto("https://www.instagram.com/")
        login(page)

        with open(OUTPUT_FILE, "a", encoding="utf-8") as out_file:
            for task in tasks:
                process_video(page, task['url'], task['category'], out_file)
                print("[...] Resting for 7 seconds to mimic human behavior...")
                time.sleep(7)

        print("\n [:D] Video processed! Check <out.txt>!")
        context.close()


if __name__ == "__main__":
    main()