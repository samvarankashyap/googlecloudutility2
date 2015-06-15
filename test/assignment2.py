_author__ = 'samvarankashyap'
import argparse
import httplib2
import os
import sys
import json
import time
import datetime
import io
import hashlib
import pdb
#Google apliclient (Google App Engine specific) libraries.
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from googleapiclient.http import MediaIoBaseDownload
#pycry#pto libraries.
from Crypto import Random
from Crypto.Cipher import AES
import httplib2
import urllib
url = "http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.csv"
password = 'mygooglepassword'
#key to use
key = hashlib.sha256(password).digest()


_BUCKET_NAME = 'aes_bucket' #name of your google bucket.
_API_VERSION = 'v1'

# Parser for command-line arguments.
parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])


# client_secret.json is the JSON file that contains the client ID and Secret.
#You can download the json file from your google cloud console.
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secret.json')

# Set up a Flow object to be used for authentication.
# Add one or more of the following scopes.
# These scopes are used to restrict the user to only specified permissions (in this case only to devstorage)
FLOW = client.flow_from_clientsecrets(CLIENT_SECRETS,
  scope=[
      'https://www.googleapis.com/auth/devstorage.full_control',
      'https://www.googleapis.com/auth/devstorage.read_only',
      'https://www.googleapis.com/auth/devstorage.read_write',
    ],
    message=tools.message_if_missing(CLIENT_SECRETS))

def put(service,filename):
    """User inputs the file name that needs to be uploaded.
       Encrypt the given file using AES encryption
       and then upload the file to your bucket on the google cloud storage.
       Remove the file from your local machine after the upload. """
    try:
        req = service.objects().insert(media_body=filename,name=filename, bucket=_BUCKET_NAME)
        resp = req.execute()
        print '>Uploaded source file %s' % filename
        print json.dumps(resp, indent=2)
        os.remove(filename)
    except client.AccessTokenRefreshError:
        print ("Error in the credentials")
    except Exception as e:
        print "File named "+filename+"is not found please try again \n"


def download_file():
    #h = httplib2.Http('.cache')
    #response, content = h.request(url)
    #print(response.status)
    #file_name = ""
    #with open('all_week.csv', 'wb') as f:
    #    f.write(content)
    #    file_name = f.name
    testfile = urllib.URLopener()
    testfile.retrieve(url, "all_week.csv")
    return "all_week.csv"

def print_hello():
    return "This is hello from print hello function"

def get_service_object():
    flags = parser.parse_args([])
    storage = file.Storage('sample.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(FLOW, storage,flags)
    http = httplib2.Http()
    http = credentials.authorize(http)
    # Construct the service object for the interacting with the Cloud Storage API.
    service = discovery.build('storage', _API_VERSION, http=http)
    flags = parser.parse_args(argv[1:])
    return service
"""
def main(argv):
  # Parse the command-line flags.
  flags = parser.parse_args(argv[1:])
  #pdb.set_trace()

  #sample.dat file stores the short lived access tokens, which your application requests user data, attaching the access token to the request.
  #so that user need not validate through the browser everytime. This is optional. If the credentials don't exist
  #or are invalid run through the native client flow. The Storage object will ensure that if successful the good
  # credentials will get written back to the file (sample.dat in this case).
  storage = file.Storage('sample.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(FLOW, storage, flags)
  # Create an httplib2.Http object to handle our HTTP requests and authorize it
  # with our good Credentials.
  http = httplib2.Http()
  http = credentials.authorize(http)
  # Construct the service object for the interacting with the Cloud Storage API.
  service = discovery.build('storage', _API_VERSION, http=http)
  #testing the get function

  #This is kind of switch equivalent in C or Java.
  #Store the option and name of the function as the key value pair in the dictionary.
  start_time = int(round(time.time() * 1000))
  file_name = download_file()
  end_time =  int(round(time.time() * 1000))
  print("--- %s seconds ---to download file" % (end_time - start_time))
  start_time = int(round(time.time() * 1000))
  put(service,file_name)
  end_time =  int(round(time.time() * 1000))
  print("--- %s seconds ---to upload file to google cloud" % (end_time - start_time))

if __name__ == '__main__':
  main(sys.argv)
# [END all]
"""
