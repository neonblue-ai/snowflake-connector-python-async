#!/usr/bin/env python3
"""
Compare sync vs async cursor behavior to identify if the issue is a regression.
"""

import asyncio
import os
import sys

# Add the local src directory to Python path to use development version
repo_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.aio import AsyncSnowflakeConnection

def test_sync_cursor_behavior():
    """Test sync cursor behavior with the exact same operations."""
    load_dotenv()
    
    # Connection parameters
    conn_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    }
    
    # Authentication
    private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    if private_key_raw:
        conn_params['private_key'] = private_key_raw
    elif private_key_path:
        conn_params['private_key_file'] = private_key_path
    elif password:
        conn_params['password'] = password
    else:
        raise ValueError("No authentication configured")
    
    print("🔄 Testing SYNC cursor behavior...")
    conn = snowflake.connector.connect(**conn_params)
    
    try:
        cursor = conn.cursor()
        test_table = "DEBUG_SYNC_CURSOR_TEST"
        
        # Set context
        cursor.execute(f"USE DATABASE {conn_params['database']}")
        cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        
        # Create table
        cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        cursor.execute(f"""
            CREATE TABLE {test_table} (
                id INTEGER,
                message VARCHAR(100)
            )
        """)
        print("   ✅ Table created")
        
        # Insert data
        cursor.execute(f"""
            INSERT INTO {test_table} (id, message) 
            VALUES (1, 'First message'), (2, 'Second message')
        """)
        print("   ✅ Data inserted")
        
        # Test COUNT with same cursor
        print("\n   📊 Testing COUNT with same cursor...")
        cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row = cursor.fetchone()
        print(f"      COUNT fetchone() result: {count_row}")
        
        # Test SELECT * with same cursor
        print("\n   📊 Testing SELECT * with same cursor...")
        cursor.execute(f"SELECT * FROM {test_table} ORDER BY id")
        all_rows = cursor.fetchall()
        print(f"      SELECT * fetchall() result: {all_rows}")
        print(f"      Number of rows: {len(all_rows)}")
        
        # Test COUNT again with same cursor
        print("\n   📊 Testing COUNT again with same cursor...")
        cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row2 = cursor.fetchone()
        print(f"      COUNT fetchone() result: {count_row2}")
        
        # Test with NEW cursor
        print("\n   📊 Testing COUNT with NEW cursor...")
        new_cursor = conn.cursor()
        new_cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row3 = new_cursor.fetchone()
        print(f"      NEW cursor COUNT result: {count_row3}")
        
        # Cleanup
        cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        
        return {
            'count_same_cursor_1': count_row,
            'select_all_rows': all_rows,
            'count_same_cursor_2': count_row2,
            'count_new_cursor': count_row3
        }
        
    finally:
        conn.close()

async def test_async_cursor_behavior():
    """Test async cursor behavior with the exact same operations."""
    load_dotenv()
    
    # Connection parameters
    conn_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    }
    
    # Authentication
    private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    if private_key_raw:
        conn_params['private_key'] = private_key_raw
    elif private_key_path:
        conn_params['private_key_file'] = private_key_path
    elif password:
        conn_params['password'] = password
    else:
        raise ValueError("No authentication configured")
    
    print("⚡ Testing ASYNC cursor behavior...")
    conn = AsyncSnowflakeConnection(**conn_params)
    await conn.connect()
    
    try:
        cursor = conn.cursor()
        test_table = "DEBUG_ASYNC_CURSOR_TEST"
        
        # Set context
        await cursor.execute(f"USE DATABASE {conn_params['database']}")
        await cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        
        # Create table
        await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        await cursor.execute(f"""
            CREATE TABLE {test_table} (
                id INTEGER,
                message VARCHAR(100)
            )
        """)
        print("   ✅ Table created")
        
        # Insert data
        await cursor.execute(f"""
            INSERT INTO {test_table} (id, message) 
            VALUES (1, 'First message'), (2, 'Second message')
        """)
        print("   ✅ Data inserted")
        
        # Test COUNT with same cursor
        print("\n   📊 Testing COUNT with same cursor...")
        await cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row = await cursor.fetchone()
        print(f"      COUNT fetchone() result: {count_row}")
        
        # Test SELECT * with same cursor
        print("\n   📊 Testing SELECT * with same cursor...")
        await cursor.execute(f"SELECT * FROM {test_table} ORDER BY id")
        all_rows = await cursor.fetchall()
        print(f"      SELECT * fetchall() result: {all_rows}")
        print(f"      Number of rows: {len(all_rows)}")
        
        # Test COUNT again with same cursor
        print("\n   📊 Testing COUNT again with same cursor...")
        await cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row2 = await cursor.fetchone()
        print(f"      COUNT fetchone() result: {count_row2}")
        
        # Test with NEW cursor
        print("\n   📊 Testing COUNT with NEW cursor...")
        new_cursor = conn.cursor()
        await new_cursor.execute(f"SELECT COUNT(*) as row_count FROM {test_table}")
        count_row3 = await new_cursor.fetchone()
        print(f"      NEW cursor COUNT result: {count_row3}")
        
        # Cleanup
        await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        
        return {
            'count_same_cursor_1': count_row,
            'select_all_rows': all_rows,
            'count_same_cursor_2': count_row2,
            'count_new_cursor': count_row3
        }
        
    finally:
        await conn.close()

async def main():
    """Compare sync vs async cursor behavior."""
    print("🚀 Comparing Sync vs Async Cursor Behavior")
    print("=" * 60)
    
    # Test sync behavior
    sync_results = test_sync_cursor_behavior()
    
    print("\n" + "=" * 60)
    
    # Test async behavior
    async_results = await test_async_cursor_behavior()
    
    print("\n" + "=" * 60)
    print("📊 COMPARISON RESULTS")
    print("=" * 60)
    
    # Compare results
    print(f"\n🔍 COUNT with same cursor (first time):")
    print(f"   Sync:  {sync_results['count_same_cursor_1']}")
    print(f"   Async: {async_results['count_same_cursor_1']}")
    print(f"   Match: {sync_results['count_same_cursor_1'] == async_results['count_same_cursor_1']}")
    
    print(f"\n🔍 SELECT * with same cursor:")
    print(f"   Sync:  {len(sync_results['select_all_rows'])} rows")
    print(f"   Async: {len(async_results['select_all_rows'])} rows")
    print(f"   Match: {sync_results['select_all_rows'] == async_results['select_all_rows']}")
    
    print(f"\n🔍 COUNT with same cursor (second time):")
    print(f"   Sync:  {sync_results['count_same_cursor_2']}")
    print(f"   Async: {async_results['count_same_cursor_2']}")
    print(f"   Match: {sync_results['count_same_cursor_2'] == async_results['count_same_cursor_2']}")
    
    print(f"\n🔍 COUNT with new cursor:")
    print(f"   Sync:  {sync_results['count_new_cursor']}")
    print(f"   Async: {async_results['count_new_cursor']}")
    print(f"   Match: {sync_results['count_new_cursor'] == async_results['count_new_cursor']}")
    
    # Analysis
    if all([
        sync_results['count_same_cursor_1'] == async_results['count_same_cursor_1'],
        sync_results['select_all_rows'] == async_results['select_all_rows'],
        sync_results['count_same_cursor_2'] == async_results['count_same_cursor_2'],
        sync_results['count_new_cursor'] == async_results['count_new_cursor']
    ]):
        print(f"\n✅ CONCLUSION: Async cursor behavior matches sync cursor exactly")
        print(f"   This suggests the issue is NOT in cursor operations but elsewhere")
    else:
        print(f"\n❌ CONCLUSION: Async cursor behavior differs from sync cursor")
        print(f"   This is a regression that needs to be fixed")

if __name__ == "__main__":
    asyncio.run(main())