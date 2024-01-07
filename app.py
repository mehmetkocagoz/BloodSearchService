from flask import Flask,request
from flask_apscheduler import APScheduler
from azure.storage.queue import QueueServiceClient
import base64
from dotenv import load_dotenv
import os
import json
import pyodbc
import logging
from mailsender import sendEmailToDonors,sendEmailToRequestor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

account_name = os.getenv('AZURE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_ACCOUNT_KEY')
queue_name = os.getenv('QUEUE_NAME')

server = os.getenv("AZURE_SERVER")
port = 1433
user = os.getenv("AZURE_ID")
password = os.getenv("AZURE_PASSWORD")
database = 'finaldatabase'

# Build connection string
conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={user};PWD={password}"
def connectDatabase():
    try:
        # Create a connection
        with pyodbc.connect(conn_str, timeout=15) as conn:
            return conn

    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        return f"Error connecting to the database. SQLState: {sqlstate}"


def connect():
    # Create a QueueServiceClient
    queue_service_client = QueueServiceClient(account_url=f"https://{account_name}.queue.core.windows.net", credential=account_key)

    # Create a QueueClient
    queue_client = queue_service_client.get_queue_client(queue_name)

    return queue_client

def takeDonorEmailList(donor_name_list):
    connection = connectDatabase()
    cursor = connection.cursor()
    donor_email_list = []
    for donor_name in donor_name_list:
        cursor.execute("SELECT email FROM Donors WHERE donor_name = ?",(donor_name,))
        donor_email = cursor.fetchone()
        donor_email = donor_email[0]
        donor_email_list.append(donor_email)
    
    return donor_email_list


def checkDatabaseForBlood(requested_blood_type,requested_unit,requestor_email):
    
    connection = connectDatabase()
    cursor = connection.cursor()
    req = requested_unit
    cursor.execute("SELECT SUM(units) FROM BloodDonations WHERE blood_type = ?",(requested_blood_type,))

    available_blood_d = cursor.fetchone()[0] or 0
    
    if available_blood_d is None:
        available_blood = 0
    else:
        available_blood = available_blood_d
    
    requested_unit = int(requested_unit)
    available_blood_d = int(available_blood_d)

    if available_blood_d >= requested_unit:
        cursor.execute("""
                    SELECT * FROM BloodDonations WHERE blood_type = ?
                """,(requested_blood_type,))
        donors = cursor.fetchall()
        blood_need = requested_unit
        i=0
        donor_name_list = []
        while (blood_need>0):
            don_ID = donors[i][0]
            donor_name = donors[i][1]
            blood_type = donors[i][2]
            unit = donors[i][3]
            i = i+1
            if unit > blood_need:
                unit -=blood_need
                blood_need = 0
                cursor.execute("""
                                UPDATE BloodDonations SET units = ? WHERE donation_id = ?;
                    """,(unit,don_ID,))
                donor_name_list.append(donor_name)
            # If donor donated equal request, we will delete donor row and set the blood_need to 0, loop will end
            elif unit == blood_need:
                blood_need = 0
                cursor.execute("""
                                DELETE FROM BloodDonations WHERE donation_id = ?
                                """,(don_ID,))               
                donor_name_list.append(donor_name)
                # Else we will try to collect requested blood units from donors, loop will continue
            else:
                blood_need = blood_need - unit
                cursor.execute("""
                                DELETE FROM BloodDonations WHERE donation_id = ?
                                """,(don_ID,))
                donor_name_list.append(donor_name)
        connection.commit()
        message = """
                Requested Blood Found!
            """
        sendEmailToRequestor(requestor_email,message)
        donor_email_list = takeDonorEmailList(donor_name_list)
        donor_message = f"Your blood, {requested_blood_type}, helped someone! Gifted Unit: {req}"
        sendEmailToDonors(donor_email_list,donor_message)
        return True
    else:
        return False
    
def requestBloodFromDatabase(blood_type,email, units):
    req = units
    connection = connectDatabase()
    cursor = connection.cursor()
    # First we will check BloodDonations table, if there is enough blood we will directly send email to requestor 'Blood Found Donor= {'donor_name'}'
    cursor.execute("""
            SELECT SUM(units) FROM BloodDonations WHERE blood_type = ?
        """, (blood_type,))
    total_units_available = cursor.fetchone()[0] or 0
    # If there is enough available units, we will try to collect blood from donors
    # It can be one or more donors therefore I used while loop inside this if clause
    if total_units_available >= units:    
        cursor.execute("""
                    SELECT * FROM BloodDonations WHERE blood_type = ?
                       """,(blood_type,))
        donors = cursor.fetchall()
        blood_need = units
        i=0
        donor_name_list = []
        while (blood_need>0):
            don_ID = donors[i][0]
            donor_name = donors[i][1]
            blood_type = donors[i][2]
            unit = donors[i][3]
            i = i+1
            # If donor donated more then requested units of blood, we will update donor's unit and set the blood_need to 0, loop will end
            if unit > blood_need:
                unit -=blood_need
                blood_need = 0
                cursor.execute("""
                            UPDATE BloodDonations SET units = ? WHERE donation_id = ?;
                               """,(unit,don_ID,))
                donor_name_list.append(donor_name)
            # If donor donated equal request, we will delete donor row and set the blood_need to 0, loop will end
            elif unit == blood_need:
                blood_need = 0
                cursor.execute("""
                            DELETE FROM BloodDonations WHERE donation_id = ?
                               """,(don_ID,))               
                donor_name_list.append(donor_name)
            # Else we will try to collect requested blood units from donors, loop will continue
            else:
                blood_need = blood_need - unit
                cursor.execute("""
                            DELETE FROM BloodDonations WHERE donation_id = ?
                               """,(don_ID,))
                donor_name_list.append(donor_name)
        connection.commit()
    # Else there is no enough blood, we will send a message to queue, queue will handled with another service
    else:
        return False
            
    connection.commit()
    connection.close()
    message = """
        Requested Blood Found!
    """
    sendEmailToRequestor(email,message)
    donor_email_list = takeDonorEmailList(donor_name_list)
    donor_message = f"Your blood, {blood_type}, helped someone! Gifted Unit: {req}"

    sendEmailToDonors(donor_email_list,donor_message)
    return True   




def dequeue_message():
    # Connect to the queue
    queue_client = connect()

    # Dequeue a message
    messages = queue_client.receive_messages()
    for message in messages:
        decoded_message = decode_message(message.content)
        '''
        # After Logic If want to delete, delete like it
        queue_client.delete_message(message)
        '''
        print(f"Dequeued message: {decoded_message}")
        requested_blood_type = decoded_message['blood_type']
        request_city = decoded_message['city']
        requestor_email = decoded_message['email']
        requested_unit = decoded_message['units']
        # If it is True, blood bank has enough units
        if requestBloodFromDatabase(requested_blood_type,requestor_email,requested_unit) == True:
            queue_client.delete_message(message)

        else:
            if decoded_message['duration'] == 1:
                queue_client.delete_message(message)

            else:
                decoded_message['duration'] -=1
                encoded_message = base64.b64encode(json.dumps(decoded_message).encode()).decode('utf-8')
                queue_client.update_message(message.id, message.pop_receipt, encoded_message, visibility_timeout=30)
                
    return "Service Worked Everything is Fine"

def decode_message(encoded_message):
    try:
        # Decode base64 and parse JSON
        decoded_message = base64.b64decode(encoded_message).decode('utf-8')
        return json.loads(decoded_message)
    except (ValueError, UnicodeDecodeError) as e:
        print(f"Error decoding message: {str(e)}")
        return None

app = Flask(__name__)

@app.route('/')
def hello_world():
    is_trigger = request.args.get('trigger') == 'true'
    if is_trigger:
        response = dequeue_message()
        return response
    else:
        return 'Not A Trigger HTTP'


@app.route('/check')
def check():
    return 'APP IS WORKING'

if __name__ == '__main__':

    app.run()
   

    