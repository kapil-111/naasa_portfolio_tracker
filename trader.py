from playwright.sync_api import Page
import time

class Trader:
    def __init__(self, page: Page, dry_run=True):
        self.page = page
        self.dry_run = dry_run

    def place_order(self, signal):
        """
        Executes an order based on the signal.
        signal: dict with keys 'side', 'symbol', 'quantity', 'price'
        """
        print(f"--- Placing Order: {signal['side']} {signal['symbol']} x {signal['quantity']} @ {signal['price']} ---")
        
        try:
            # Navigate to Order Page if not there
            if "MarketOrder/Order" not in self.page.url:
                print("Navigating to Order Page...")
                self.page.goto("https://x.naasasecurities.com.np/MarketOrder/Order")
                self.page.wait_for_load_state("networkidle")

            # 1. Select Buy/Sell
            # HTML Structure:
            # <div class="sl_by">
            #    <a ...>SELL</a>
            #    <a ...>BUY</a>
            # </div>
            if signal['side'].upper() == 'BUY':
                print("Selecting BUY side...")
                self.page.click(".sl_by a:has-text('BUY')")
            else:
                print("Selecting SELL side...")
                self.page.click(".sl_by a:has-text('SELL')")
            
            # Wait for UI update
            self.page.wait_for_timeout(500)

            # 2. Enter Symbol
            # <input id="searchStock" ...>
            print(f"Entering symbol: {signal['symbol']}")
            self.page.fill("#searchStock", signal['symbol'])
            self.page.wait_for_timeout(1000) 
            self.page.press("#searchStock", "Enter") # Select from dropdown

            # 3. Enter Quantity
            # <input id="OrdertxtQty" ...>
            print(f"Entering quantity: {signal['quantity']}")
            self.page.fill("#OrdertxtQty", str(signal['quantity']))
            
            # 4. Enter Price
            # <input id="OrdertxtPrice" ...>
            print(f"Entering price: {signal['price']}")
            self.page.fill("#OrdertxtPrice", str(signal['price']))

            # 5. Submit
            # <a id="btnBuy" ...>Buy</a> (Note: Check if ID changes to btnSell or stays btnBuy)
            # Safe bet: use the button in the submit area
            submit_button = self.page.locator("#btnBuy")
            
            if self.dry_run:
                print(f"[DRY RUN] Order form filled for {signal['symbol']}. NOT submitting.")
            else:
                print("Submitting order...")
                submit_button.click()
                self.page.wait_for_timeout(2000)
                self.page.screenshot(path="order_result.png")
                print("Order submitted. Screenshot saved.")
            return True
                
        except Exception as e:
            print(f"Error placing order: {e}")
            self.page.screenshot(path="order_error.png")
            return False

