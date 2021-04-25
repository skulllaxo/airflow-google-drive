
import os
import sys

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class Driver():

    def __init__(self):
        self.gauth = GoogleAuth()
        # Try to load saved client credentials

    
    def authenticate(self):
        self.gauth.LoadCredentialsFile("mycreds.txt")

        if self.gauth.credentials is None:
            # Authenticate if they're not there
            self.gauth.LocalWebserverAuth()
        elif self.gauth.access_token_expired:
            # Refresh them if expired
            self.gauth.Refresh()
        else:
            # Initialize the saved creds
            self.gauth.Authorize()
        # Save the current credentials to a file
        self.gauth.SaveCredentialsFile("mycreds.txt")

    def upload_file(self):
        self.authenticate()
        drive = GoogleDrive(self.gauth)

        textfile = drive.CreateFile()
        textfile.SetContentFile('eng.txt')
        textfile.Upload()

drive = Driver()
drive.upload_file()