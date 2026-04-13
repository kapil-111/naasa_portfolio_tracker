from playwright.sync_api import Page

from naasa_locators import (
    naasa_order,
    order_quantity_input,
    order_side_buy,
    order_side_sell,
    order_submit_button,
    order_success_indicators,
    order_symbol_input,
    order_type_mkt,
    wait_after_side_select,
    wait_after_symbol_entry,
    wait_for_order_page,
)


class Trader:
    def __init__(self, page: Page, dry_run=True):
        self.page = page
        self.dry_run = dry_run
        self.last_error = ""

    def place_order(self, signal):
        print(f"--- Placing Order: {signal['side']} {signal['symbol']} x {signal['quantity']} ---")

        try:
            if "MarketOrder/Order" not in self.page.url:
                print("Navigating to Order Page...")
                self.page.goto(naasa_order())
                wait_for_order_page(self.page)

            side = signal["side"].upper()
            if side == "BUY":
                print("Selecting BUY...")
                order_side_buy(self.page).first.click()
            else:
                print("Selecting SELL...")
                order_side_sell(self.page).first.click()
            wait_after_side_select(self.page)

            print(f"Entering symbol: {signal['symbol']}")
            sym = order_symbol_input(self.page)
            sym.fill(signal["symbol"])
            self.page.wait_for_timeout(400)
            sym.press("Enter")
            wait_after_symbol_entry(self.page)

            print("Selecting MKT order type...")
            order_type_mkt(self.page).first.click()
            self.page.wait_for_timeout(200)

            print(f"Entering quantity: {signal['quantity']}")
            order_quantity_input(self.page).fill(str(signal["quantity"]))

            submit_button = order_submit_button(self.page)

            if self.dry_run:
                print(f"[DRY RUN] MKT order form filled for {signal['symbol']}. NOT submitting.")
                return True

            print("Submitting order...")
            submit_button.click()
            try:
                order_success_indicators(self.page).first.wait_for(state="visible", timeout=5000)
                self.page.screenshot(path="order_result.png")
                print("Order confirmed by broker.")
                return True
            except Exception:
                self.page.screenshot(path="order_result.png")
                print("Warning: No broker confirmation detected. Order may or may not have been placed. Screenshot saved.")
                return True

        except Exception as e:
            self.last_error = str(e)
            print(f"Error placing order: {e}")
            self.page.screenshot(path="order_error.png")
            return False
