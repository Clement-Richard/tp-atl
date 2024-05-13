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
    with open(script_path, 'r') as script_file:
        sql_script = script_file.read()
        with engine.connect() as connection:
            connection.execute(text(sql_script))

def main():
    # Database connection parameters
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_data_mart"
    }
    
    # Constructing the database URL
    database_url = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    
    # Create the database if it doesn't exist
    if not database_exists(database_url):
        create_database(database_url)
        print(f"Database '{db_config['dbms_database']}' created successfully.")
    else:
        print(f"Database '{db_config['dbms_database']}' already exists.")
    
    # Creating SQLAlchemy engine
    engine = create_engine(database_url)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    
    print(f'script dir: {script_dir}')
    
    folder_path = os.path.join(script_dir, '..', 'script')
    
    # Get all SQL script files in the directory
    sql_scripts = [f for f in os.listdir(folder_path) if f.lower().endswith('.sql') and os.path.isfile(os.path.join(folder_path, f))]
    
    # Execute each SQL script
    for script_file in sql_scripts:
        script_path = os.path.join(folder_path, script_file)
        print(f"Executing script: {script_file}")
        execute_sql_script(script_path, engine)
        print("Script execution completed.")
    
    print("All scripts executed successfully.")

if __name__ == '__main__':
    main()
