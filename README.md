# MLGW Download

This repository is an Azure Function that downloads meter readings from Memphis Light Gas & Water and saves it to a CSV file in Azure Blob Storage

## Logic

When the function is executed it gets retrieves the data from the previous day and appends it to files in the specified Azure Storage Container.

The data is saved into files based on the meter name.

If no file exists for the meter one is created with a header line

There is no logic to prevent duplicate data. So if the function is run more than once a day duplicate data will be collected.

## Required Environment Variables

How to set environment variables in Azure Functions for [local](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local#local-settings-file) testing or [productive](https://docs.microsoft.com/en-us/azure/azure-functions/functions-how-to-use-azure-function-app-settings) use

|Variable|description|
|---|---|
|MLGW_USERNAME| Your MLGW Username |
|MLGW_PASSWORD| Your MLGW Password|
|STORAGE_CONNECTIONSTRING|Connection string used to connect to the desired Azure Storage Account|
|STORAGE_CONTAINER| Name of the storage container inside the Azure storage account. Note: Container will needs to exist before script execution |

