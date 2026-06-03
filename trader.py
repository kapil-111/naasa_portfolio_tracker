import time

from playwright.sync_api import Page

from naasa_locators import (
    dismiss_any_confirmation,
    goto_broker_page,
    naasa_order,
    order_amo_range_price,
    order_quantity_input,
    order_side_buy,
    order_side_sell,
    order_submit_button,
    order_symbol_input,
    order_type_amo,
    wait_for_order_page,
)
from notifications import notify_order_screenshot


class Trader:
    def __init__(self, page: Page, dry_run=True):
        self.page = page
        self.dry_run = dry_run
        self.last_error = ""
        # success | failure | unconfirmed | None
        self.last_outcome = None

    def place_order(self, signal):
        symbol   = signal["symbol"]
        side     = signal["side"].upper()
        quantity = str(signal["quantity"])

        print(f"--- Placing Order: {side} {symbol} x {quantity} ---")
        self.last_outcome = None
        self.last_error   = ""

        try:
            # Navigate to order page if not already there
            if "MarketOrder/Order" not in self.page.url:
                goto_broker_page(self.page, naasa_order())
                wait_for_order_page(self.page)

            # Step 1: Click BUY or SELL toggle
            print(f"Step 1: Click {side}")
            if side == "BUY":
                order_side_buy(self.page).first.click()
            else:
                order_side_sell(self.page).first.click()
            self.page.wait_for_timeout(500)

            # Step 2: Type symbol + Enter
            print(f"Step 2: Symbol {symbol}")
            sym = order_symbol_input(self.page)
            sym.click()
            sym.fill(symbol)
            self.page.wait_for_timeout(400)
            sym.press("Enter")
            # Wait for quantity field to appear (symbol loaded)
            order_quantity_input(self.page).wait_for(state="visible", timeout=8_000)
            self.page.wait_for_timeout(500)

            # Step 3: Type quantity
            print(f"Step 3: Quantity {quantity}")
            qty = order_quantity_input(self.page)
            qty.click()
            qty.fill("")
            qty.type(quantity)
            self.page.wait_for_timeout(300)

            # Step 4: Click MKT label
            print("Step 4: Click MKT")
            self.page.locator("label[for='chkOrderTypeMKT']").click()
            self.page.wait_for_timeout(500)

            # Step 5: Click submit button
            submit_button = order_submit_button(self.page)

            if self.dry_run:
                self.page.screenshot(path="order_before.png")
                notify_order_screenshot("order_before.png", "📋 DRY RUN — not submitted", symbol, side)
                print(f"[DRY RUN] Form filled for {symbol}. NOT submitting.")
                self.last_outcome = None
                return True

            self.page.on("dialog", lambda d: d.accept())
            self.page.screenshot(path="order_before.png")
            notify_order_screenshot("order_before.png", "📋 Before Submit", symbol, side)

            print("Step 5: Submit order")
            submit_button.click()
            dismiss_any_confirmation(self.page, timeout_ms=3_000)

            # Wait 800ms — error toast is visible in this window
            self.page.wait_for_timeout(800)
            self.page.screenshot(path="order_result.png")

            # Detect outcome
            outcome = "unconfirmed"
            detail  = ""
            err_loc = self.page.locator(".alert-danger, .toast-error, .toast-danger, .invalid-feedback, .text-danger")
            try:
                if err_loc.count() > 0 and err_loc.first.is_visible():
                    outcome = "failure"
                    detail  = err_loc.first.inner_text(timeout=500).strip()
            except Exception:
                pass
            if outcome == "unconfirmed":
                try:
                    if qty.input_value(timeout=300).strip() == "":
                        outcome = "success"
                except Exception:
                    pass

            if outcome == "success":
                self.last_outcome = "success"
                notify_order_screenshot("order_result.png", "✅ Order Accepted", symbol, side)
                print("Order accepted by broker.")
                return True

            if outcome == "failure":
                self.last_outcome = "failure"
                self.last_error   = detail or "Broker rejected the order."
                notify_order_screenshot("order_result.png", f"❌ Order REJECTED: {self.last_error}", symbol, side)
                print(f"Order rejected: {self.last_error}")
                return False

            # Unconfirmed — qty never reset, no error shown
            self.last_outcome = "unconfirmed"
            self.last_error   = "Order submitted but no confirmation or error received. Verify manually."
            notify_order_screenshot("order_result.png", "⚠️ Order UNCONFIRMED — verify manually", symbol, side)
            print("Warning: order outcome unclear — verify in broker portal.")
            return False

        except Exception as e:
            self.last_error = str(e)
            print(f"Error placing order: {e}")
            try:
                self.page.screenshot(path="order_error.png")
            except Exception:
                pass
            return False

    def place_amo_order(self, signal):
        """
        Place an AMO (After Market Order) conditional order.
        signal must include 'symbol', 'side', 'quantity', 'price' (limit price),
        and 'amo_range_price' (trigger threshold — LTP condition).
        """
        symbol      = signal["symbol"]
        side        = signal["side"].upper()
        quantity    = str(signal["quantity"])
        price       = str(signal["price"])
        range_price = str(signal.get("amo_range_price", signal["price"]))

        print(f"--- Placing AMO Order: {side} {symbol} x{quantity} @ {price} (trigger {range_price}) ---")
        self.last_outcome = None
        self.last_error   = ""

        try:
            if "MarketOrder/Order" not in self.page.url:
                goto_broker_page(self.page, naasa_order())
                wait_for_order_page(self.page)

            # Step 1: BUY / SELL toggle
            print(f"Step 1: Click {side}")
            if side == "BUY":
                order_side_buy(self.page).first.click()
            else:
                order_side_sell(self.page).first.click()
            self.page.wait_for_timeout(500)

            # Step 2: Symbol
            print(f"Step 2: Symbol {symbol}")
            sym = order_symbol_input(self.page)
            sym.click()
            sym.fill(symbol)
            self.page.wait_for_timeout(400)
            sym.press("Enter")
            order_quantity_input(self.page).wait_for(state="visible", timeout=8_000)
            self.page.wait_for_timeout(500)

            # Step 3: Quantity
            print(f"Step 3: Quantity {quantity}")
            qty = order_quantity_input(self.page)
            qty.click()
            qty.fill("")
            qty.type(quantity)
            self.page.wait_for_timeout(300)

            # Step 4: Select AMO order type
            print("Step 4: Click AMO")
            order_type_amo(self.page).click()
            self.page.wait_for_timeout(500)

            # Step 5: Fill price field (AMO uses limit price — no separate trigger field)
            print(f"Step 5: Price {price}")
            try:
                price_field = self.page.locator("#OrdertxtPrice").or_(
                    self.page.locator("input[id*='txtPrice']:not([disabled])")
                ).first
                price_field.wait_for(state="visible", timeout=5_000)
                if price_field.is_enabled():
                    price_field.click()
                    price_field.fill("")
                    price_field.type(price)
                    self.page.wait_for_timeout(300)
            except Exception as e:
                print(f"[AMO] Price field fill skipped: {e}")

            submit_button = order_submit_button(self.page)

            if self.dry_run:
                self.page.screenshot(path="amo_order_before.png")
                notify_order_screenshot("amo_order_before.png", "📋 DRY RUN AMO — not submitted", symbol, side)
                print(f"[DRY RUN] AMO form filled for {symbol}. NOT submitting.")
                self.last_outcome = None
                return True

            self.page.on("dialog", lambda d: d.accept())
            self.page.screenshot(path="amo_order_before.png")
            notify_order_screenshot("amo_order_before.png", "📋 AMO Before Submit", symbol, side)

            print("Step 7: Submit AMO order")
            submit_button.click()
            dismiss_any_confirmation(self.page, timeout_ms=3_000)

            self.page.wait_for_timeout(800)
            self.page.screenshot(path="amo_order_result.png")

            outcome = "unconfirmed"
            detail  = ""
            err_loc = self.page.locator(".alert-danger, .toast-error, .toast-danger, .invalid-feedback, .text-danger")
            try:
                if err_loc.count() > 0 and err_loc.first.is_visible():
                    outcome = "failure"
                    detail  = err_loc.first.inner_text(timeout=500).strip()
            except Exception:
                pass
            if outcome == "unconfirmed":
                try:
                    if qty.input_value(timeout=300).strip() == "":
                        outcome = "success"
                except Exception:
                    pass

            if outcome == "success":
                self.last_outcome = "success"
                notify_order_screenshot("amo_order_result.png", "✅ AMO Order Accepted", symbol, side)
                print("AMO order accepted by broker.")
                return True

            if outcome == "failure":
                self.last_outcome = "failure"
                self.last_error   = detail or "Broker rejected the AMO order."
                notify_order_screenshot("amo_order_result.png", f"❌ AMO REJECTED: {self.last_error}", symbol, side)
                print(f"AMO order rejected: {self.last_error}")
                return False

            self.last_outcome = "unconfirmed"
            self.last_error   = "AMO submitted but no confirmation or error received. Verify manually."
            notify_order_screenshot("amo_order_result.png", "⚠️ AMO UNCONFIRMED — verify manually", symbol, side)
            print("Warning: AMO outcome unclear — verify in broker portal.")
            return False

        except Exception as e:
            self.last_error = str(e)
            print(f"Error placing AMO order: {e}")
            try:
                self.page.screenshot(path="amo_order_error.png")
            except Exception:
                pass
            return False
