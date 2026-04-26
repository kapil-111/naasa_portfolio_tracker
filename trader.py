from playwright.sync_api import Page

from naasa_locators import (
    dismiss_any_confirmation,
    goto_broker_page,
    naasa_order,
    order_quantity_input,
    order_side_buy,
    order_side_sell,
    order_submit_button,
    order_symbol_input,
    poll_order_submission_outcome,
    wait_after_side_select,
    wait_after_symbol_entry,
    wait_for_order_page,
)
from notifications import notify_order_screenshot


class Trader:
    def __init__(self, page: Page, dry_run=True):
        self.page = page
        self.dry_run = dry_run
        self.last_error = ""
        # success | failure | unconfirmed | None (not submitted / dry run)
        self.last_outcome = None

    def place_order(self, signal):
        print(f"--- Placing Order: {signal['side']} {signal['symbol']} x {signal['quantity']} ---")
        self.last_outcome = None
        self.last_error = ""

        try:
            if "MarketOrder/Order" not in self.page.url:
                print("Navigating to Order Page...")
                goto_broker_page(self.page, naasa_order())
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
            # Check the MKT radio and call the page's own ClickOrderType() handler
            self.page.evaluate("""() => {
                const r = document.querySelector('#chkOrderTypeMKT');
                if (r) {
                    r.checked = true;
                    if (typeof ClickOrderType === 'function') ClickOrderType();
                }
            }""")
            self.page.wait_for_timeout(300)

            print(f"Entering quantity: {signal['quantity']}")
            qty_input = order_quantity_input(self.page)
            qty_input.click()
            qty_input.fill("")
            self.page.wait_for_timeout(100)
            qty_input.type(str(signal["quantity"]))
            # Force the framework to recognise the value via JS events
            self.page.evaluate(
                """(val) => {
                    const el = document.querySelector('#OrdertxtQty');
                    if (!el) return;
                    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    nativeInputValueSetter.call(el, val);
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                str(signal["quantity"])
            )
            self.page.wait_for_timeout(400)

            submit_button = order_submit_button(self.page)

            if self.dry_run:
                print(f"[DRY RUN] MKT order form filled for {signal['symbol']}. NOT submitting.")
                self.last_outcome = None
                return True

            # Auto-accept browser-native confirm() dialogs (window.confirm, window.alert)
            self.page.on("dialog", lambda d: d.accept())

            self.page.screenshot(path="order_before.png")
            notify_order_screenshot("order_before.png", "📋 Before Submit", signal["symbol"], side)

            print("Submitting order...")
            submit_button.click()

            # Auto-dismiss any HTML modal/overlay confirmation dialog
            dismiss_any_confirmation(self.page, timeout_ms=3_000)

            outcome, detail = poll_order_submission_outcome(self.page)
            self.page.screenshot(path="order_result.png")

            if outcome == "success":
                self.last_outcome = "success"
                notify_order_screenshot("order_result.png", "✅ Order Accepted", signal["symbol"], side)
                print("Order confirmed by broker.")
                return True

            if outcome == "failure":
                self.last_outcome = "failure"
                self.last_error = detail or "Broker reported an error."
                notify_order_screenshot("order_result.png", f"❌ Order REJECTED: {self.last_error}", signal["symbol"], side)
                print(f"Order failed: {self.last_error}")
                return False

            self.last_outcome = "unconfirmed"
            self.last_error = (
                "UNCONFIRMED: qty field did not reset and no error appeared after submit. "
                "Order may or may not have executed — verify in broker portal. Screenshot: order_result.png"
            )
            notify_order_screenshot("order_result.png", "⚠️ Order UNCONFIRMED — verify manually", signal["symbol"], side)
            print(
                "Warning: Order outcome unclear — qty field never reset (possible non-submission). "
                "Not treating as success — verify manually. Screenshot saved."
            )
            return False

        except Exception as e:
            self.last_error = str(e)
            print(f"Error placing order: {e}")
            self.page.screenshot(path="order_error.png")
            return False
