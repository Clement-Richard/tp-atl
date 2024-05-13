import os
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database

def execute_sql_script(script_path: str, engine: create_engine):
    """
    Execute the SQL script located at the given path using the provided SQLAlchemy engine.
    
    Parameters:
        - script_path (str): Path to the SQL script file.
        - engine (create_engine): SQLAlchemy engine instance.
    """
    try:
        with open(script_path, 'r') as script_file:
            sql_script = text(script_file.read())
            print(f"Executing SQL script: {script_path}")
            with engine.connect() as connection:
                connection.execute(sql_script)
            print(f"SQL script execution completed: {script_path}")
    except Exception as e:
        print(f"Error executing SQL script {script_path}: {e}")

def main():
    # Database connection parameters
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_datamart"
    }
    
    # Constructing the database URL
    database_url = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    
    try:
        # Check if the database exists
        if not database_exists(database_url):
            # Create the database if it doesn't exist
            create_database(database_url)
            print(f"Database '{db_config['dbms_database']}' created successfully.")
        else:
            print(f"Database '{db_config['dbms_database']}' already exists.")
        
        # Check database access rights
        engine = create_engine(database_url)
        with engine.connect():
            print("Database connection successful.")
            
            # Fetch the SQL scripts directory path
            script_dir = os.path.dirname(os.path.abspath(__file__))
            folder_path = os.path.join(script_dir, '..', 'script')
            
            # Check if the directory containing SQL scripts exists
            if os.path.exists(folder_path):
                # Get all SQL script files in the directory
                sql_scripts = [f for f in os.listdir(folder_path) if f.lower().endswith('.sql') and os.path.isfile(os.path.join(folder_path, f))]
                
                # Execute each SQL script
                for script_file in sql_scripts:
                    script_path = os.path.join(folder_path, script_file)
                    print(f"Executing script: {script_file}")
                    execute_sql_script(script_path, engine)
                    print("Script execution completed.")
            else:
                print("Directory containing SQL scripts not found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
