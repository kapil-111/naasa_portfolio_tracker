import asyncio
from nepse import Client

async def main():
    try:
        # Initialize Client
        client = Client()
        
        print("Fetching historical data for NABIL...")
        
        # 1. Get Company Details to find ID
        # The correct method is likely in security_client
        try:
             # Based on inspection, get_company takes a symbol
            company = await client.security_client.get_company("NABIL")
            print(f"Found NABIL (ID: {company.security_id})")
            
            # 2. Get History
            history = await client.security_client.get_company_history(company.security_id)
            
            print(f"Fetched {len(history)} records.")
            for record in history[:5]:
                print(f"Date: {record.business_date}, Close: {record.close_price}")
                
        except Exception as e:
            print(f"Error fetching data: {e}")
        finally:
             await client.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
