import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, classification_report
import boto3
import ibm_db
import snowflake.connector
import teradatasql
import cx_Oracle
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)






#dummy connection to snowflake database

def fetch_snowflake_data():
    try:
        conn = snowflake.connector.connect(
            account='ope_snowflake_demo',
            user='ope@snowflake.com',
            password='ope_password_demo',
            warehouse='raw_call_center.DB',
            database='call_center.DB',
            schema='call_center_DB_schema'
        )
        
        query = "SELECT * FROM call_logs"
        
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
        
        cur.close()
        conn.close()
        
        logger.info(f"Fetched {len(df)} rows from Snowflake")
        return df
    except Exception as e:
        logger.error(f"Error fetching Snowflake data: {str(e)}")
        return pd.DataFrame()

#dummy connection to teradata database

def fetch_teradata_data():
    try:
        conn = teradatasql.connect(host='127.0.0.1', user='ope@teradata.com', password='ope_password_demo')
        
        query = "SELECT * FROM agent_performance"
        
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
        
        conn.close()
        
        logger.info(f"Fetched {len(df)} rows from Teradata")
        return df
    except Exception as e:
        logger.error(f"Error fetching Teradata data: {str(e)}")
        return pd.DataFrame()
    
#dummy connection to oracle database

def fetch_oracle_data():
    try:
        conn = cx_Oracle.connect('ope@oracle.com/ope_password_demo@127.0.0.1:5050/service')
        
        query = "SELECT * FROM customer_satisfaction"
        
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=[col[0] for col in cur.description])
        
        cur.close()
        conn.close()
        
        logger.info(f"Fetched {len(df)} rows from Oracle")
        return df
    except Exception as e:
        logger.error(f"Error fetching Oracle data: {str(e)}")
        return pd.DataFrame()
    
    #dummy connection to aws database

    def fetch_aws_data():
    try:
        rds_client = boto3.client('rds')
        query = "SELECT * FROM customer_interactions"
        result = rds_client.execute_statement(ResourceArn='ope_resource_arn', SecretArn='ope_secret_arn', Sql=query)
        return pd.DataFrame(result['Records'])
    except Exception as e:
        logger.error(f"Error fetching AWS data: {str(e)}")
        return pd.DataFrame()
    
#dummy connection to ibm db2 database

def fetch_db2_data():
    try:
        conn = ibm_db.connect("DATABASE=call_center.DB;HOSTNAME=127.0.0.1;PORT=5050;PROTOCOL=TCPIP;UID=ope@db2.com;PWD=ope_password_demo;", "", "")
        query = "SELECT * FROM customer_feedback"
        stmt = ibm_db.exec_immediate(conn, query)
        result = ibm_db.fetch_assoc(stmt)
        return pd.DataFrame(result)
    except Exception as e:
        logger.error(f"Error fetching DB2 data: {str(e)}")
        return pd.DataFrame()





def combine_data():
    dfs = [fetch_aws_data(), fetch_db2_data(), fetch_snowflake_data(), fetch_teradata_data(), fetch_oracle_data()]
    combined_data = pd.concat(dfs, axis=0, ignore_index=True)
    logger.info(f"Combined data shape: {combined_data.shape}")
    return combined_data

def preprocess_data(data):
    # Handle missing values
    data = data.dropna()
    
    # Encode categorical variables
    categorical_columns = data.select_dtypes(include=['object']).columns
    data = pd.get_dummies(data, columns=categorical_columns)
    
    # Normalize numerical columns
    numerical_columns = data.select_dtypes(include=['int64', 'float64']).columns
    scaler = StandardScaler()
    data[numerical_columns] = scaler.fit_transform(data[numerical_columns])
    
    logger.info(f"Preprocessed data shape: {data.shape}")
    return data

# training the machine learning model
def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=45)
    
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=45))
    ])
 #pipeline fitting   
    pipeline.fit(X_train, y_train)
    
    y_pred = pipeline.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"Model Accuracy: {accuracy}")
    logger.info("\nClassification Report:\n", classification_report(y_test, y_pred))
    
    return pipeline



# call center model integration

class CallCenterReceiver:
    def __init__(self, model):
        self.model = model
    
    def receive_call(self, call_data):
        prediction = self.model.predict(call_data)
        logger.info(f"Call received. Prediction: {prediction}")
        return prediction

class CallCenterCaller:
    def make_call(self, phone_number):
        logger.info(f"Calling {phone_number}")
        # Add logic for handling the call

class CallCenterScheduler:
    def __init__(self):
        self.schedule = {}
    
    def schedule_call(self, phone_number, date_time):
        self.schedule[phone_number] = date_time
        logger.info(f"Call scheduled for {phone_number} at {date_time}")
    
    def get_upcoming_calls(self):
        now = datetime.now()
        upcoming = {phone: dt for phone, dt in self.schedule.items() if now <= dt <= now + timedelta(days=1)}
        return upcoming
    




    def main():
    # Fetch and preprocess data
    combined_data = combine_data()
    processed_data = preprocess_data(combined_data)
    
    # Prepare features and target variable
    X = processed_data.drop('target_column', axis=1)
    y = processed_data['target_column']
    
    # Train the model
    model = train_model(X, y)
    
    # Initialize call center components
    receiver = CallCenterReceiver(model)
    caller = CallCenterCaller()
    scheduler = CallCenterScheduler()
    
    # How it will work
    incoming_call_data = np.array([[1, 2, 3, 4, 5]])  #dummy call data array
    prediction = receiver.receive_call(incoming_call_data)
    
    caller.make_call("336-456-7890")
    
    scheduler.schedule_call("336-654-3210", datetime.now() + timedelta(hours=2))
    upcoming_calls = scheduler.get_upcoming_calls()
    logger.info(f"Upcoming calls: {upcoming_calls}")

if __name__ == "__main__":
    main()