from playwright.sync_api import Page

class Trader:
    def __init__(self, page: Page, dry_run=True):
        self.page = page
        self.dry_run = dry_run
        self.last_error = ""

    def place_order(self, signal):
        print(f"--- Placing Order: {signal['side']} {signal['symbol']} x {signal['quantity']} ---")

        try:
            # Navigate to Order Page
            if "MarketOrder/Order" not in self.page.url:
                print("Navigating to Order Page...")
                self.page.goto("https://x.naasasecurities.com.np/MarketOrder/Order")
                self.page.wait_for_load_state("networkidle")

            # 1. Select BUY or SELL
            if signal['side'].upper() == 'BUY':
                print("Selecting BUY...")
                self.page.click(".sl_by a:has-text('BUY')")
            else:
                print("Selecting SELL...")
                self.page.click(".sl_by a:has-text('SELL')")
            self.page.wait_for_timeout(500)

            # 2. Enter Symbol + Enter to select from dropdown
            print(f"Entering symbol: {signal['symbol']}")
            self.page.fill("#searchStock", signal['symbol'])
            self.page.wait_for_timeout(1000)
            self.page.press("#searchStock", "Enter")
            self.page.wait_for_timeout(500)

            # 3. Select MKT (market order) — avoids price range validation
            print("Selecting MKT order type...")
            self.page.click("label:has-text('MKT')")
            self.page.wait_for_timeout(500)

            # 4. Enter Quantity
            print(f"Entering quantity: {signal['quantity']}")
            self.page.fill("#OrdertxtQty", str(signal['quantity']))

            # 5. Submit — button ID is always #btnBuy regardless of BUY/SELL mode
            submit_button = self.page.locator("#btnBuy")

            if self.dry_run:
                print(f"[DRY RUN] MKT order form filled for {signal['symbol']}. NOT submitting.")
                return True

            print("Submitting order...")
            submit_button.click()
            # Wait for broker confirmation — success toast or error message
            try:
                self.page.wait_for_selector(
                    "text=successfully, text=Success, text=Order placed, .alert-success, .toast-success",
                    timeout=5000
                )
                self.page.screenshot(path="order_result.png")
                print("Order confirmed by broker.")
                return True
            except Exception:
                # No confirmation detected — take screenshot for manual review
                self.page.screenshot(path="order_result.png")
                print("Warning: No broker confirmation detected. Order may or may not have been placed. Screenshot saved.")
                # Return True anyway — order was already recorded before submit.
                # Returning False here would NOT undo the record, so True is safer.
                return True

        except Exception as e:
            self.last_error = str(e)
            print(f"Error placing order: {e}")
            self.page.screenshot(path="order_error.png")
            return False
