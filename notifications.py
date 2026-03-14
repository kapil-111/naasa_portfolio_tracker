import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_notification(signal, is_dry_run=False):
    """
    Sends an email notification when an order is placed (or simulated).
    """
    sender_email = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")
    receiver_email = os.getenv("RECEIVER_EMAIL")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port_str = os.getenv("SMTP_PORT")

    if not all([sender_email, sender_password, receiver_email, smtp_server, smtp_port_str]):
        print("Email configuration missing. Skipping notification.")
        return False

    smtp_port = int(smtp_port_str)  # type: ignore[arg-type]

    v_side = signal.get('side', 'UNKNOWN')
    v_symbol = signal.get('symbol', 'UNKNOWN')
    v_qty = signal.get('quantity', 0)
    v_price = signal.get('price', 0)

    mode_text = "[DRY_RUN] " if is_dry_run else "[LIVE] "
    subject = f"{mode_text}Order Placed: {v_side} {v_qty} {v_symbol}"

    body = f"""
Portfolio Tracker Notification

An order has been {'simulated' if is_dry_run else 'placed'} by the trading bot:

Action: {v_side}
Symbol: {v_symbol}
Quantity: {v_qty}
Price: {v_price}

Mode: {'DRY RUN (No real order submitted)' if is_dry_run else 'LIVE (Real order submitted to broker)'}
    """

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)  # type: ignore[arg-type]
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print(f"Email notification sent to {receiver_email}")
        return True
    except Exception as e:
        print(f"Failed to send email notification: {e}")
        return False

if __name__ == "__main__":
    # Test script if run directly
    from dotenv import load_dotenv
    load_dotenv()
    test_signal = {'side': 'BUY', 'symbol': 'TEST', 'quantity': 100, 'price': 1000}
    send_email_notification(test_signal, is_dry_run=True)
