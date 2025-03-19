#!/usr/bin/env python3

import os
import csv
import asyncio
import aiohttp
import signal
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Global flag for graceful shutdown
shutdown_event = asyncio.Event()

# Define account sets
ACCOUNTS = [
    {
        'name': os.getenv('ACCOUNT_NAME1'),
        'address': os.getenv('TELLOR_ADDRESS1'),
        'valoper_address': os.getenv('TELLORVALOPER_ADDRESS1')
    },
    {
        'name': os.getenv('ACCOUNT_NAME2'),
        'address': os.getenv('TELLOR_ADDRESS2'),
        'valoper_address': os.getenv('TELLORVALOPER_ADDRESS2')
    },
    {
        'name': os.getenv('ACCOUNT_NAME3'),
        'address': os.getenv('TELLOR_ADDRESS3'),
        'valoper_address': os.getenv('TELLORVALOPER_ADDRESS3')
    },
    {
        'name': os.getenv('ACCOUNT_NAME4'),
        'address': os.getenv('TELLOR_ADDRESS4'),
        'valoper_address': os.getenv('TELLORVALOPER_ADDRESS4')
    }
]

# Check if any required variables are missing
for account in ACCOUNTS:
    if not all(account.values()):
        raise ValueError(f"Missing required environment variables for account {account['name']}. Please check your .env file.")

async def fetch_endpoint(session, endpoint, column, extract_func):
    try:
        async with session.get(endpoint) as response:
            print(f"\nFetching {column} from: {endpoint}")
            print(f"Status code: {response.status}")
            
            if response.status != 200:
                error_text = await response.text()
                print(f"Error response: {error_text}")
                return column, ''
            
            data = await response.json()
            print(f"Response data: {data}")
            
            value = extract_func(data)
            print(f"Extracted value: {value}")
            
            return column, value
            
    except Exception as e:
        print(f"Error processing {column}: {str(e)}")
        return column, ''

async def fetch_data_for_account(account):
    base_url = "https://info.layer-node.com"
    
    print(f"\nFetching data for account: {account['name']}")
    print(f"Address: {account['address']}")
    print(f"Valoper Address: {account['valoper_address']}")
    
    endpoints = [
        (f"{base_url}/cosmos/bank/v1beta1/balances/{account['address']}/by_denom?denom=loya", 
         'account_balance', lambda x: x['balance']['amount']),
        
        (f"{base_url}/cosmos/distribution/v1beta1/delegators/{account['address']}/rewards",
         'rewards', lambda x: sum(float(reward['amount']) for reward in x.get('total', []))),
        
        (f"{base_url}/cosmos/distribution/v1beta1/validators/{account['valoper_address']}/outstanding_rewards",
         'outstanding_rewards', lambda x: x['rewards']['rewards'][0]['amount']),
        
        (f"{base_url}/tellor-io/layer/reporter/available-tips/{account['address']}",
         'available-tips', lambda x: sum(float(tip['amount']['amount']) for tip in x.get('tips', []))),
        
        (f"{base_url}/cosmos/staking/v1beta1/validators/{account['valoper_address']}",
         'delegator_shares', lambda x: x['validator']['delegator_shares']),
        
        (f"{base_url}/cosmos/staking/v1beta1/delegations/{account['address']}",
         'delegations', lambda x: sum(float(delegation['balance']['amount']) for delegation in x['delegation_responses']))
    ]
    
    results = {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_endpoint(session, endpoint, column, extract_func) 
                for endpoint, column, extract_func in endpoints]
        responses = await asyncio.gather(*tasks)
        
        for column, value in responses:
            results[column] = value
    
    return results

def save_to_csv(data, filename):
    file_exists = Path(filename).exists()
    
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(data)

async def monitor_account(account):
    filename = f"{account['name']}.csv"
    
    while not shutdown_event.is_set():
        try:
            data = await fetch_data_for_account(account)
            save_to_csv(data, filename)
            print(f"Data saved to {filename} at {data['timestamp']}")
            await asyncio.sleep(300)  # Wait 5 minutes
            
        except Exception as e:
            print(f"Error in monitoring loop for {account['name']}: {str(e)}")
            await asyncio.sleep(60)  # Wait 1 minute before retry

async def main_loop():
    # Create monitoring tasks for all accounts
    tasks = [monitor_account(account) for account in ACCOUNTS]
    # Run all monitoring tasks concurrently
    await asyncio.gather(*tasks)

def signal_handler(signum, frame):
    print("\nShutting down gracefully...")
    shutdown_event.set()

def main():
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        print("Monitoring stopped.")

if __name__ == "__main__":
    main() 