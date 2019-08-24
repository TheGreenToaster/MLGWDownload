import requests 
import re
import json
import zipfile
import io
import csv
import os
import shutil
import logging
from azure.storage.blob import BlockBlobService, PublicAccess
from urllib.parse import urlparse, parse_qs



def downloadMeterData(mlgwUsername,mlgwPassword,readingDate,storageConnectionString,storageContainer):

    # Initialize blob service
    blobService = BlockBlobService(connection_string=storageConnectionString)

    # Initial URL used to authenticate the user
    loginURL='https://secure3.billerweb.com/mlg/inetSrv'

    # Using the session object should allow for better header and cookie management
    s = requests.Session() 

    logging.info('Logging in to MLGW')

    # Post the user credentials to the logon page
    resp = s.post(loginURL,allow_redirects=False,data={'type': 'SignonService','client': '713120700','pageOnly': 'true','unitCode': 'MLG','loginid': mlgwUsername,'password': mlgwPassword})

    # Follow the redirect
    resp2 = s.get(resp.headers['location'],allow_redirects=False)

    # Parse the query URL to get the session guid to use in the new session on the metering site
    url = urlparse(resp2.headers['location'])
    query = parse_qs(url.query)
    sessionGuid = query['SessionGuid'][0]

    # Using a new session because it is a new site
    s2 = requests.Session()

    # Authenticate using the session guid from the billing site
    url2 = 'https://ce.aclarasw.com/Suite/LA/Home?referrerid={}&CustomerId={}&PremiseId=1339704&NxAuthGuid={}'.format(query['referrerid'][0],mlgwUsername,sessionGuid)
    logging.info('Navigating to Meter info')
    s2.get(url2)

    # Initialize Export Dialog to avoid errors later
    resp4 = s2.get('https://ce.aclarasw.com/Suite/LA/GreenButton/Index/1')

    # Parse the HTML to get the meters listed on the account
    logging.info('Gathering Meters')
    meters = re.findall(r'<option role="option" value="([^"]+)">',resp4.text)
    
    # Create tmp directory to work the magic in
    os.mkdir('tmp')

    # Looping through the meters we found
    for meter in meters:
        #Ask the site to prepare the download
        resp5 = s2.post('https://ce.aclarasw.com/Suite/LA/GreenButton/ExportAmiData',headers={'content-type': 'application/json; charset=UTF-8','Accept':'application/json, text/javascript, */*; q=0.01'},data='{"MeterId":"' + meter + '","StartDate":"'+ readingDate +'","EndDate":"'+ readingDate +'"}')
        resp5json = json.loads(resp5.text)

        # Download the results
        resp6 = s2.get('https://ce.aclarasw.com/Suite/LA/GreenButton/DownloadFile?file=&name={}.{}'.format(resp5json['FileName'],resp5json['FileType']))
        csv_zip = zipfile.ZipFile(io.BytesIO(resp6.content))

        # Create meter file name
        meterBlobName = meter + '.csv'

        # If the file is not created in the storage container we want to include the header row. If not we want to just append to the existing file
        writeHeader=False
        try:
            blobService.get_blob_to_path(storageContainer,meterBlobName,'tmp/'+meterBlobName)
        except:
            writeHeader = True

        # Site should have only returned a zip file with 1 file in it
        zipFilehandler = csv_zip.open(csv_zip.namelist()[0])
        
        # Open the output file
        blobFileHandler = open('tmp/' + meterBlobName,'ab+')

        # Use the header logic to determine how many lines to skip
        if writeHeader:
            skipLines = 14
        else:
            skipLines = 15

        # MLGW puts a 14 line header with customer info in it that we dont want so we skip those lines in the read
        for x in zipFilehandler.readlines()[skipLines:]:
            # Write lines to output file
            blobFileHandler.write(x)

        blobFileHandler.close()

        # Upload the output file to the Azure Storage Container
        blobService.create_blob_from_path(storageContainer,meterBlobName,'tmp/'+meterBlobName)

        logging.info("Wrote meter data for {} to Azure Storage".format(meter))

    # Cleanup
    shutil.rmtree('tmp')

