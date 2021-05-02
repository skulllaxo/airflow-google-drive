
import os
import sys

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#https://dados.educacao.sp.gov.br/dataset/endere%C3%A7os-de-escolas
class Drive():

    def __init__(self):
        self.gauth = GoogleAuth()
        self.HOME = os.getenv('HOME')
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

    def upload_file(self,folder_id,file_name,title):
        self.authenticate()
        drive = GoogleDrive(self.gauth)

        textfile = drive.CreateFile({'parents': [{'id':folder_id}],'title':title})
        textfile.SetContentFile(file_name)
        textfile.Upload()

if __name__ == '__main__':
    drive = Drive()
    drive.upload_file(folder_id ='1wCQlfB-q1Aj6AceKz5RLBExMo9bJFutc',file_name = 'load_files/Result.csv',title = 'ETL_school.csv')
