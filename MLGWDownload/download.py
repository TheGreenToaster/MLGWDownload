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
    try:
        resp = s.post(loginURL,allow_redirects=False,data={'type': 'SignonService','client': '713120700','pageOnly': 'true','unitCode': 'MLG','loginid': mlgwUsername,'password': mlgwPassword})
        logging.info('Login succeeded')
    except:
        logging.error('Login failed')
        exit()

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
    logging.info('Navigating to Meter info using url:{}'.format(url2))
    resp3 = s2.get(url2)
    logging.info('Response 3:{}'.format(resp3.text))

    # Initialize Export Dialog to avoid errors later
    resp4 = s2.get('https://ce.aclarasw.com/Suite/LA/GreenButton/Index/1')

    # Parse the HTML to get the meters listed on the account
    logging.info('Response 4:{}'.format(resp4.text))
    meters = re.findall(r'<option role="option" value="([^"]+)">',resp4.text)
    
    logging.info('found {} meters'.format(len(meters)))

    # Create tmp directory to work the magic in
    logging.info('createing temp directory')
    os.mkdir('tmp')

    # Looping through the meters we found
    for meter in meters:
        #Ask the site to prepare the download
        logging.info('requesting download for meter:{}'.format(meter))
        resp5 = s2.post('https://ce.aclarasw.com/Suite/LA/GreenButton/ExportAmiData',headers={'content-type': 'application/json; charset=UTF-8','Accept':'application/json, text/javascript, */*; q=0.01'},data='{"MeterId":"' + meter + '","StartDate":"'+ readingDate +'","EndDate":"'+ readingDate +'"}')
        
        logging.info('Download request result:{}'.format(resp5.text))
        resp5json = json.loads(resp5.text)

        # Download the results
        logging.info('downloading file {}.{} for meter:{}'.format(resp5json['FileName'],resp5json['FileType'],meter))
        resp6 = s2.get('https://ce.aclarasw.com/Suite/LA/GreenButton/DownloadFile?file=&name={}.{}'.format(resp5json['FileName'],resp5json['FileType']))
        csv_zip = zipfile.ZipFile(io.BytesIO(resp6.content))

        # Create meter file name
        meterBlobName = meter + '.csv'

        # If the file is not created in the storage container we want to include the header row. If not we want to just append to the existing file
        writeHeader=False
        try:
            blobService.get_blob_to_path(storageContainer,meterBlobName,'tmp/'+meterBlobName)
            logging.info('Meter file found for meter {}. Will append to existing file'.format(meter))
        except:
            writeHeader = True
            logging.info('Meter file not found in storage account. Will create new file with header line.')

        # Site should have only returned a zip file with 1 file in it
        logging.info('Unzipping the returned file for meter:{}'.format(meter))
        zipFilehandler = csv_zip.open(csv_zip.namelist()[0])
        
        # Open the output file
        logging.info('Opening the {} file for edit'.format(meter))
        blobFileHandler = open('tmp/' + meterBlobName,'ab+')

        # Use the header logic to determine how many lines to skip
        if writeHeader:
            skipLines = 14
        else:
            skipLines = 15

        logging.info('Skipping the first {} lines of downloaded file'.format(skipLines))

        # MLGW puts a 14 line header with customer info in it that we dont want so we skip those lines in the read
        lines = zipFilehandler.readlines()[skipLines:]
        print('Writing {} lines to {}'.format(len(lines),meterBlobName))
        for x in lines:
            # Write lines to output file
            print(x)
            blobFileHandler.write(x)

        logging.info('Closing file {}'.format(meterBlobName))
        blobFileHandler.close()

        # Upload the output file to the Azure Storage Container
        logging.info('Uploading file {} to {} in container {}'.format('tmp/'+meterBlobName,meterBlobName,storageContainer))
        blobService.create_blob_from_path(storageContainer,meterBlobName,'tmp/'+meterBlobName)

        # logging.info("Wrote meter data for {} to Azure Storage".format(meter))

    # Cleanup
    logging.info('Cleaning up tmp directory')
    shutil.rmtree('tmp')

