import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
from mupifDB import restApiControl

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import json


def sendEmail(receiver_address, subject, message):
    try:
        username = ''
        password = ''
        server = ''
        with open("/var/lib/mupif/persistent/mupif-smtp-credentials.json") as json_data_file:
            credentials = json.load(json_data_file)
            username = credentials['username']
            password = credentials['password']
            server = credentials['server']

        sender_address = username
        sender_pass = password
        # Setup the MIME
        msg = MIMEMultipart()
        msg['From'] = sender_address
        msg['To'] = receiver_address
        msg['Subject'] = subject
        # The body and the attachments for the mail
        msg.attach(MIMEText(message, 'plain'))
        # Create SMTP session for sending the mail
        session = smtplib.SMTP(server)
        session.starttls()  # enable security
        session.login(sender_address, sender_pass)  # login with mail_id and password
        text = msg.as_string()
        session.sendmail(sender_address, receiver_address, text)
        session.quit()
        return True
    except:
        return False


def sendEmailAboutExecutionStatus(eid):
    web_server = 'http://' + str(os.environ.get('MUPIF_NS', "http://127.0.0.1:9090")).replace('http://', '').split(':')[0] + ':5555/'

    execution = restApiControl.getExecutionRecord(eid)
    if execution.RequestedBy != '':
        text = f'Your execution of workflow "{execution.WorkflowID}" (version {execution.WorkflowVersion}) is now in Status "{execution.Status}". You can view its detail here: {web_server}workflowexecutions/{eid}'
        if execution.Status == 'Created':
            text += ' It has probably reached the limit of attempts for execution while some resources were not available.'
        return sendEmail(execution.RequestedBy, 'MuPIF DB execution info', text)
    return False
