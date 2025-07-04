#!/usr/bin/env python3
"""
Debug script to test async transaction operations in isolation.
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

async def test_simple_async_operations():
    """Test basic async operations to debug transaction issues."""
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
    
    print("🔌 Connecting to Snowflake...")
    conn = AsyncSnowflakeConnection(**conn_params)
    await conn.connect()
    
    try:
        print("📊 Testing basic async operations...")
        
        cursor = conn.cursor()
        
        # Test 1: Simple query
        print("\n1. Testing basic query...")
        await cursor.execute(f"USE DATABASE {conn_params['database']}")
        await cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        await cursor.execute("SELECT 1 as test_value")
        row = await cursor.fetchone()
        print(f"   Query result: {row}")
        
        # Test 2: Create table and insert data
        print("\n2. Testing table operations...")
        test_table = "DEBUG_ASYNC_TRANSACTION_TEST"
        
        # Clean up first
        await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        
        # Create table
        await cursor.execute(f"""
            CREATE TABLE {test_table} (
                id INTEGER,
                description VARCHAR(100)
            )
        """)
        print("   ✅ Table created")
        
        # Test autocommit mode
        print("\n3. Testing autocommit...")
        await conn.autocommit(True)
        print("   ✅ Autocommit enabled")
        
        # Insert data
        await cursor.execute(f"""
            INSERT INTO {test_table} (id, description) 
            VALUES (1, 'Test data')
        """)
        print("   ✅ Data inserted")
        
        # Count rows
        await cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        row = await cursor.fetchone()
        count = int(row[0]) if row else 0
        print(f"   Row count: {count}")
        
        # Test manual transaction
        print("\n4. Testing manual transaction...")
        await conn.autocommit(False)
        print("   ✅ Autocommit disabled")
        
        # Insert more data
        await cursor.execute(f"""
            INSERT INTO {test_table} (id, description) 
            VALUES (2, 'Transaction data')
        """)
        print("   ✅ Transaction data inserted")
        
        # Count before commit
        await cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        row = await cursor.fetchone()
        count_before = int(row[0]) if row else 0
        print(f"   Row count before commit: {count_before}")
        
        # Commit
        await conn.commit()
        print("   ✅ Transaction committed")
        
        # Count after commit
        await cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        row = await cursor.fetchone()
        count_after = int(row[0]) if row else 0
        print(f"   Row count after commit: {count_after}")
        
        # Test rollback
        print("\n5. Testing rollback...")
        
        # Insert data to rollback
        await cursor.execute(f"""
            INSERT INTO {test_table} (id, description) 
            VALUES (3, 'Rollback data')
        """)
        print("   ✅ Rollback data inserted")
        
        # Count before rollback
        await cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        row = await cursor.fetchone()
        count_before_rollback = int(row[0]) if row else 0
        print(f"   Row count before rollback: {count_before_rollback}")
        
        # Rollback
        await conn.rollback()
        print("   ✅ Transaction rolled back")
        
        # Count after rollback
        await cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        row = await cursor.fetchone()
        count_after_rollback = int(row[0]) if row else 0
        print(f"   Row count after rollback: {count_after_rollback}")
        
        # Summary
        print(f"\n📊 SUMMARY:")
        print(f"   - Autocommit insert worked: {count > 0}")
        print(f"   - Manual commit worked: {count_after > count_before}")
        print(f"   - Rollback worked: {count_after_rollback < count_before_rollback}")
        
        # Cleanup
        await cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        print(f"   ✅ Cleanup completed")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_simple_async_operations())