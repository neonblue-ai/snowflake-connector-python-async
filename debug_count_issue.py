#!/usr/bin/env python3
"""Debug script to reproduce the COUNT(*) issue."""

import asyncio
import os
import sys
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from snowflake.connector.aio.connection import AsyncSnowflakeConnection


async def test_count_issue():
    """Test the COUNT(*) issue."""
    
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    }
    
    # Filter out None values
    connection_params = {k: v for k, v in connection_params.items() if v is not None}
    
    if not connection_params:
        print("No connection parameters found. Please set SNOWFLAKE_* environment variables.")
        return
    
    print(f"Connecting with account: {connection_params['account']}")
    
    # Create connection
    async_conn = AsyncSnowflakeConnection(**connection_params)
    
    try:
        # Connect
        await async_conn.connect()
        print("Connected successfully!")
        
        # Create cursor
        async with async_conn.cursor() as cursor:
            # Test COUNT(*) query
            print("\n=== Testing COUNT(*) query ===")
            await cursor.execute("SELECT COUNT(*) FROM information_schema.tables")
            
            # Check result metadata
            print(f"Description: {cursor.description}")
            print(f"Rowcount: {cursor.rowcount}")
            
            # Check if async result set exists
            print(f"Has async result set: {cursor._async_result_set is not None}")
            if cursor._async_result_set:
                print(f"Result set batches: {len(cursor._async_result_set.batches)}")
                for i, batch in enumerate(cursor._async_result_set.batches):
                    print(f"  Batch {i}: rowcount={batch.rowcount}, local={batch._local}")
                    if batch._local and batch._data:
                        print(f"    Local data: {batch._data}")
            
            # Try fetchone
            result = await cursor.fetchone()
            print(f"fetchone() result: {result}")
            
            # Test SELECT * query for comparison
            print("\n=== Testing SELECT * query ===")
            await cursor.execute("SELECT table_name, table_schema FROM information_schema.tables LIMIT 5")
            
            print(f"Description: {cursor.description}")
            print(f"Rowcount: {cursor.rowcount}")
            
            # Check if async result set exists
            print(f"Has async result set: {cursor._async_result_set is not None}")
            if cursor._async_result_set:
                print(f"Result set batches: {len(cursor._async_result_set.batches)}")
                for i, batch in enumerate(cursor._async_result_set.batches):
                    print(f"  Batch {i}: rowcount={batch.rowcount}, local={batch._local}")
                    if batch._local and batch._data:
                        print(f"    Local data (first 3): {batch._data[:3] if len(batch._data) > 3 else batch._data}")
            
            # Try fetchall
            results = await cursor.fetchall()
            print(f"fetchall() results: {results}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await async_conn.close()


if __name__ == "__main__":
    asyncio.run(test_count_issue())