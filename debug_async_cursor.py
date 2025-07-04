#!/usr/bin/env python3
"""
Debug script to test async cursor operations in detail.
"""

import asyncio
import os
import sys

# Add the local src directory to Python path to use development version
repo_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
from snowflake.connector.aio import AsyncSnowflakeConnection

async def test_async_cursor_operations():
    """Debug async cursor execute and fetch operations."""
    load_dotenv()
    
    # Connection parameters
    conn_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    }
    
    # Authentication - prefer private key over password
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
    
    print("🔌 Connecting to Snowflake with async connector...")
    conn = AsyncSnowflakeConnection(**conn_params)
    await conn.connect()
    
    try:
        print("🧪 Testing async cursor operations step by step...")
        
        cursor = conn.cursor()
        
        # Set context
        print("\n1. Setting database context...")
        result1 = await cursor.execute(f"USE DATABASE {conn_params['database']}")
        print(f"   USE DATABASE result: {result1}")
        
        result2 = await cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        print(f"   USE SCHEMA result: {result2}")
        
        # Test simple query
        print("\n2. Testing simple SELECT...")
        result3 = await cursor.execute("SELECT 'hello' as greeting, 42 as number")
        print(f"   SELECT result: {result3}")
        
        row = await cursor.fetchone()
        print(f"   Fetched row: {row}")
        
        # Test CREATE TABLE
        print("\n3. Testing CREATE TABLE...")
        test_table = "DEBUG_CURSOR_TEST"
        
        # Drop first
        result4 = await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        print(f"   DROP TABLE result: {result4}")
        
        # Create
        create_sql = f"""
            CREATE TABLE {test_table} (
                id INTEGER,
                message VARCHAR(100)
            )
        """
        result5 = await cursor.execute(create_sql)
        print(f"   CREATE TABLE result: {result5}")
        
        # Test INSERT
        print("\n4. Testing INSERT...")
        insert_sql = f"""
            INSERT INTO {test_table} (id, message) 
            VALUES (1, 'First message'), (2, 'Second message')
        """
        result6 = await cursor.execute(insert_sql)
        print(f"   INSERT result: {result6}")
        
        # Test SELECT COUNT
        print("\n5. Testing SELECT COUNT...")
        count_sql = f"SELECT COUNT(*) as row_count FROM {test_table}"
        result7 = await cursor.execute(count_sql)
        print(f"   COUNT result: {result7}")
        
        count_row = await cursor.fetchone()
        print(f"   Count row: {count_row}")
        count_value = int(count_row[0]) if count_row else 0
        print(f"   Parsed count: {count_value}")
        
        # Test SELECT all rows
        print("\n6. Testing SELECT all rows...")
        select_sql = f"SELECT * FROM {test_table} ORDER BY id"
        result8 = await cursor.execute(select_sql)
        print(f"   SELECT result: {result8}")
        
        all_rows = await cursor.fetchall()
        print(f"   All rows: {all_rows}")
        print(f"   Number of rows fetched: {len(all_rows)}")
        
        # Test with explicit autocommit
        print("\n7. Testing explicit autocommit...")
        await conn.autocommit(True)
        print("   ✅ Autocommit enabled")
        
        # Insert more data
        insert_sql2 = f"""
            INSERT INTO {test_table} (id, message) 
            VALUES (3, 'Autocommit message')
        """
        result9 = await cursor.execute(insert_sql2)
        print(f"   Autocommit INSERT result: {result9}")
        
        # Count again
        result10 = await cursor.execute(count_sql)
        print(f"   COUNT after autocommit result: {result10}")
        
        count_row2 = await cursor.fetchone()
        count_value2 = int(count_row2[0]) if count_row2 else 0
        print(f"   Count after autocommit: {count_value2}")
        
        # Check cursor state
        print("\n8. Checking cursor state...")
        print(f"   Cursor rowcount: {cursor.rowcount}")
        print(f"   Cursor description: {cursor.description}")
        
        # Cleanup
        result11 = await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        print(f"   DROP TABLE cleanup result: {result11}")
        
        # Summary
        print(f"\n📊 ASYNC CURSOR DEBUG SUMMARY:")
        print(f"   - Simple SELECT worked: {row is not None}")
        print(f"   - CREATE TABLE worked: {result5 is not None}")
        print(f"   - INSERT worked: {result6 is not None}")
        print(f"   - Initial count: {count_value}")
        print(f"   - All rows fetched: {len(all_rows)}")
        print(f"   - Autocommit insert worked: {count_value2 > count_value}")
        
    except Exception as e:
        print(f"❌ Error during cursor testing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_async_cursor_operations())