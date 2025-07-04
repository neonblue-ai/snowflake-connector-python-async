#!/usr/bin/env python3
"""
Debug script to verify sync transaction operations work correctly.
"""

import os
import sys

# Add the local src directory to Python path to use development version
repo_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def test_sync_transactions():
    """Test basic sync transaction operations to verify they work."""
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
    
    print("🔌 Connecting to Snowflake with sync connector...")
    conn = snowflake.connector.connect(**conn_params)
    
    try:
        print("📊 Testing sync transaction operations...")
        
        # Test the exact pattern you mentioned
        print("\n1. Creating test table...")
        conn.cursor().execute("create or replace table testtbl(a int, b string)")
        print("   ✅ Table created")
        
        # Test successful transaction
        print("\n2. Testing successful transaction...")
        conn.cursor().execute("begin")
        try:
            conn.cursor().execute("insert into testtbl(a,b) values(3, 'test3'), (4,'test4')", timeout=10)
            print("   ✅ Insert successful")
        except ProgrammingError as e:
            print(f"   ❌ Insert failed: {e}")
            conn.cursor().execute("rollback")
            print("   ✅ Rolled back")
        else:
            conn.cursor().execute("commit")
            print("   ✅ Committed")
        
        # Check row count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM testtbl")
        count = cursor.fetchone()[0]
        print(f"   Row count after transaction: {count}")
        
        # Test rollback scenario
        print("\n3. Testing rollback scenario...")
        conn.cursor().execute("begin")
        try:
            # Insert some data
            conn.cursor().execute("insert into testtbl(a,b) values(5, 'test5'), (6,'test6')")
            print("   ✅ Insert successful - about to rollback")
            # Simulate error by raising exception
            raise ProgrammingError("Simulated error")
        except ProgrammingError as e:
            print(f"   ✅ Caught error: {e}")
            conn.cursor().execute("rollback")
            print("   ✅ Rolled back")
        else:
            conn.cursor().execute("commit")
        
        # Check row count (should be same as before)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM testtbl")
        count_after_rollback = cursor.fetchone()[0]
        print(f"   Row count after rollback: {count_after_rollback}")
        
        # Test autocommit mode
        print("\n4. Testing autocommit mode...")
        conn.autocommit(True)
        print("   ✅ Autocommit enabled")
        
        conn.cursor().execute("insert into testtbl(a,b) values(7, 'autocommit_test')")
        print("   ✅ Insert with autocommit")
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM testtbl")
        count_autocommit = cursor.fetchone()[0]
        print(f"   Row count after autocommit insert: {count_autocommit}")
        
        # Test connection.commit() and connection.rollback() methods
        print("\n5. Testing connection commit/rollback methods...")
        conn.autocommit(False)
        print("   ✅ Autocommit disabled")
        
        initial_cursor = conn.cursor()
        initial_cursor.execute("SELECT COUNT(*) FROM testtbl")
        initial_count = initial_cursor.fetchone()[0]
        print(f"   Initial count: {initial_count}")
        
        # Insert data
        conn.cursor().execute("insert into testtbl(a,b) values(8, 'connection_test')")
        print("   ✅ Insert executed")
        
        # Check count before commit
        before_cursor = conn.cursor()
        before_cursor.execute("SELECT COUNT(*) FROM testtbl")
        before_count = before_cursor.fetchone()[0]
        print(f"   Count before commit: {before_count}")
        
        # Use connection.commit() method
        conn.commit()
        print("   ✅ Connection.commit() called")
        
        # Check count after commit
        after_cursor = conn.cursor()
        after_cursor.execute("SELECT COUNT(*) FROM testtbl")
        after_count = after_cursor.fetchone()[0]
        print(f"   Count after commit: {after_count}")
        
        # Test connection.rollback() method
        conn.cursor().execute("insert into testtbl(a,b) values(9, 'rollback_test')")
        print("   ✅ Insert for rollback test")
        
        before_rb_cursor = conn.cursor()
        before_rb_cursor.execute("SELECT COUNT(*) FROM testtbl")
        before_rb_count = before_rb_cursor.fetchone()[0]
        print(f"   Count before rollback: {before_rb_count}")
        
        # Use connection.rollback() method
        conn.rollback()
        print("   ✅ Connection.rollback() called")
        
        after_rb_cursor = conn.cursor()
        after_rb_cursor.execute("SELECT COUNT(*) FROM testtbl")
        after_rb_count = after_rb_cursor.fetchone()[0]
        print(f"   Count after rollback: {after_rb_count}")
        
        # Summary
        print(f"\n📊 SYNC TRANSACTION SUMMARY:")
        print(f"   - Manual transaction commit worked: {count > 0}")
        print(f"   - Manual rollback worked: {count_after_rollback == count}")
        print(f"   - Autocommit worked: {count_autocommit > count_after_rollback}")
        print(f"   - Connection.commit() worked: {after_count > before_count}")
        print(f"   - Connection.rollback() worked: {after_rb_count < before_rb_count}")
        
        # Cleanup
        conn.cursor().execute("DROP TABLE IF EXISTS testtbl")
        print(f"   ✅ Cleanup completed")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()

if __name__ == "__main__":
    test_sync_transactions()