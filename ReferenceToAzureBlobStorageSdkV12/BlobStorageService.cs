// www.craftedforeveryone.com
// Licensed under the MIT License. See LICENSE file in the project root for full license information.  

/* This project gives you a reference guide to Azure.Storage.Blobs SDK v12 with the most frequently performed actions like
 * 1.  Creating a container if does not exists
 * 2.  Add new or update access policies to container
 * 3.  Remove access policy if exists in a container
 * 4.  Set public level access policy to container
 * 5.  Remove public access level from container
 * 6.  Deleting a storage container if exists
 * 7.  Upload blob to container
 * 8.  Upload blob to a folder in container
 * 9.  Delete a blob from container
 * 10. Get Blob Properties
 * 11. Get AdHoc Blob SAS Token
 * 12. Get Access Policy Based Blob SAS Token
 * 13. Get AdHoc Container SAS Token
 * 14. Get Access Policy Based Container SAS Token
 * 15. Get Account name, container name, and blob name from URL
 * 16. Copy blob from one container to another or from one account to another
 * 17. List all files/blob from a container
 * 
 * We are using Azure Blob Storage Emulator for demo purposes in this project.
 * You can download the storage emulator at https://go.microsoft.com/fwlink/?linkid=717179&clcid=0x409 (Link is subjected to change by Microsoft. At present this link provides direct download to storage emulator)
 * To visualize the blob storage you can use Azure Blob Storage Explorer which can be downloaded at https://azure.microsoft.com/en-us/features/storage-explorer/
 */

using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using ByteSizeLib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace ReferenceToAzureBlobStorageSdkV12
{
    class BlobStorageService
    {
        string connectionString;

        public BlobStorageService(string connectionString)
        {
            this.connectionString = connectionString;
        }

        /// <summary>
        /// Create a new container
        /// </summary>
        /// <param name="containerName">Container names can contain only lowercase letters, numbers, and hyphens, and must begin and end with a letter or a number. The name can't contain two consecutive hyphens. </param>
        public void CreateContainerIfDoesNotExists(string containerName)
        {
            //Get reference to Blob Container Client using connection string and container name to be created. 
            //Container will not be created at this point. Container will be created only after executing Create() or CreateIfNotExists() method
            var container = new BlobContainerClient(connectionString, containerName);
            container.CreateIfNotExists();

            //container.Create(); //Use this method if you want to create container if it does not exists and throw exception if it already exists 
        }

        public void DeleteContainerIfExists(string containerName)
        {
            new BlobContainerClient(connectionString, containerName).DeleteIfExists();
        }

        /// <summary>
        /// Add or update access policy to container.
        /// The order to set permissions string is racwdl <seealso cref="BlobAccessPolicy.Permissions"/>
        /// r - Read
        /// a - Add
        /// c - Create
        /// w - Write
        /// d - Delete
        /// l - List
        /// Mandatory field 
        /// <seealso cref="BlobSignedIdentifier.Id">BlobSignedIdentifier.Id </seealso>
        /// </summary>
        /// <remarks>
        /// Throws error XML specified is not syntactically valid. 400 - Invalid XML Document for the following reasons
        /// 1. In case if BlobSignedIdentifier.Id <seealso cref="BlobSignedIdentifier.Id"/> is not specified
        /// 2. The order in which you specify the permissions string in BlobAccessPolicy.Permissions <seealso cref="BlobAccessPolicy.Permissions"/> is not in correct order
        /// </remarks>
        /// <param name="containerName"></param>
        public void AddNewOrUpdateAccessPoliciesToContainer(string containerName)
        {

            var container = new BlobContainerClient(connectionString, containerName);

            var accessPolicies = new List<BlobSignedIdentifier>();
            accessPolicies.Add(
                new BlobSignedIdentifier
                {
                    Id = "Read_Write_Policy",
                    AccessPolicy = new BlobAccessPolicy
                    {
                        Permissions = "rw",
                        StartsOn = DateTime.UtcNow,
                        ExpiresOn = DateTime.UtcNow.AddDays(1)
                    }
                });

            accessPolicies.Add(new BlobSignedIdentifier
            {
                Id = "Full_Access_Policy",
                AccessPolicy = new BlobAccessPolicy
                {
                    Permissions = "racwdl",
                    StartsOn = DateTime.UtcNow,
                    ExpiresOn = DateTime.UtcNow.AddDays(1)
                }
            });

            container.SetAccessPolicy(permissions: accessPolicies);
        }

        public void RemoveAccessPolicyIfExists(string containerName, string policyName)
        {
            var container = new BlobContainerClient(connectionString, containerName);

            var exisitingPolicies = container.GetAccessPolicy().Value.SignedIdentifiers.ToList();
            var policyToBeRemoved = exisitingPolicies.SingleOrDefault(x => x.Id == policyName);

            if (policyToBeRemoved == null)
                return;

            exisitingPolicies.Remove(policyToBeRemoved);
            container.SetAccessPolicy(permissions: exisitingPolicies);

        }

        public void SetPublicAccessLevel(string containerName)
        {
            var container = new BlobContainerClient(connectionString, containerName);
            container.SetAccessPolicy(PublicAccessType.BlobContainer); //Set PublicAccessType.Blob if public access is only for blob
        }

        public void RemovePublicAccessLevel(string containerName)
        {
            var container = new BlobContainerClient(connectionString, containerName);
            container.SetAccessPolicy(PublicAccessType.None);
        }

        /// <summary>
        /// Upload or overwrite blob. The blob name and the name in file path need not be same.
        /// The file from path will be uploaded and its name will be set as value passed for blobName
        /// To upload a file inside a folder in container, prefix blobName with folder names. example "folder1/uploadedfile.txt"
        /// "folder1/uploadedfile.txt" - This will create a folder named folder1 and will upload the blob uploadedfile.txt in this folder
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="filePath"></param>
        public void UploadBlob(string containerName, string blobName, string filePath)
        {
            var blob = new BlobClient(connectionString, containerName, blobName);
            blob.Upload(filePath, overwrite: true);
        }

        public void DeleteBlob(string containerName, string blobName)
        {
            new BlobClient(connectionString, containerName, blobName).DeleteIfExists();
        }

        public void DownloadBlob(string containerName, string blobName, string pathToDownload)
        {

            new BlobClient(connectionString, containerName, blobName).DownloadTo(pathToDownload);

            //Use the below code to download as stream and save to disk
            //var blobDownloadInfo = new BlobClient(connectionString, containerName, blobName).Download().Value;


            //using (var downloadFileStream = File.OpenWrite(pathToDownload))
            //{
            //    blobDownloadInfo.Content.CopyTo(downloadFileStream);
            //    downloadFileStream.Close();
            //}

        }

        public void GetBlobProperties(string containerName, string blobName)
        {
            var blob = new BlobClient(connectionString, containerName, blobName);
            var properties = blob.GetProperties().Value;
            Console.WriteLine($"File size {ByteSize.FromBytes(properties.ContentLength).MebiBytes.ToString("#.##")} MB");
            Console.WriteLine($"Content type {properties.ContentType}");
            Console.WriteLine($"Created On {properties.CreatedOn}");
            Console.WriteLine($"Updated On {properties.LastModified}");
        }


        private string GetKeyValueFromConnectionString(string key)
        {
            IDictionary<string, string> settings = new Dictionary<string, string>();
            var splitted = connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var nameValue in splitted)
            {
                var splittedNameValue = nameValue.Split(new char[] { '=' }, 2);
                settings.Add(splittedNameValue[0], splittedNameValue[1]);
            }

            return settings[key];
        }

        public void GetAdHocBlobSasToken(string containerName, string blobName)
        {
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = containerName,
                BlobName = blobName,
                Resource = "b",//Value b is for generating token for a Blob and c is for container
                StartsOn = DateTime.UtcNow.AddMinutes(-2),
                ExpiresOn = DateTime.UtcNow.AddMinutes(10),
            };

            sasBuilder.SetPermissions(BlobSasPermissions.Read | BlobSasPermissions.Write); //multiple permissions can be added by using | symbol

            var sasToken = sasBuilder.ToSasQueryParameters(new StorageSharedKeyCredential(GetKeyValueFromConnectionString("AccountName"), GetKeyValueFromConnectionString("AccountKey")));

            Console.WriteLine($"{new BlobClient(connectionString, containerName, blobName).Uri}?{sasToken}");
        }

        public void GetAccessPolicyBasedSasToken(string containerName, string blobName, string accessPolicyName)
        {
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = containerName,
                BlobName = blobName,
                Resource = "b",//Value b is for generating token for a Blob and c is for container
                Identifier = accessPolicyName
            };

            /* Note: 
             * When you are generating SAS token based on access policy assigned to a container you cannot set the token start or expiry time.
             * The application will not throw any error even if you pass start and end time to the sasBuilder and a token also will get generated
             * When you try to access the generated URL you will get the error message as 
             * "Access policy fields can be associated with signature or SAS identifier but not both"
             * Previous version of SDK's was allowing to use both access policy and modifying the time to access policy
             */
            var sasToken = sasBuilder.ToSasQueryParameters(new StorageSharedKeyCredential(GetKeyValueFromConnectionString("AccountName"), GetKeyValueFromConnectionString("AccountKey")));
            Console.WriteLine($"{new BlobClient(connectionString, containerName, blobName).Uri}?{sasToken}");
        }

        public void GetAdHocContainerSasToken(string containerName)
        {
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = containerName,
                Resource = "c", //Value b is for generating token for a Blob and c is for container
                StartsOn = DateTime.UtcNow.AddMinutes(-2),
                ExpiresOn = DateTime.UtcNow.AddMinutes(10),
            };

            sasBuilder.SetPermissions(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.Write | BlobContainerSasPermissions.List); //multiple permissions can be added by using | symbol

            var sasToken = sasBuilder.ToSasQueryParameters(new StorageSharedKeyCredential(GetKeyValueFromConnectionString("AccountName"), GetKeyValueFromConnectionString("AccountKey")));


            //Console.WriteLine($"{new BlobContainerClient(connectionString, containerName).Uri}?{sasToken}");
            Console.WriteLine($"{new BlobContainerClient(connectionString, containerName).Uri}?{sasToken}&restype=container&comp=list");

            /* Note : If you want to list the items inside container and view those details in a browser based on the generated SAS token
             * then two additional query parameters has to be appended to the token
             * the Query parameters are "restype=container&comp=list"
             */
        }

        public void GetAccessPolicyBasedContainerSasToken(string containerName,string accessPolicy)
        {
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = containerName,
                Resource = "c", //Value b is for generating token for a Blob and c is for container
                StartsOn = DateTime.UtcNow.AddMinutes(-2),
                ExpiresOn = DateTime.UtcNow.AddMinutes(10),
            };

            sasBuilder.SetPermissions(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.Write | BlobContainerSasPermissions.List); //multiple permissions can be added by using | symbol

            var sasToken = sasBuilder.ToSasQueryParameters(new StorageSharedKeyCredential(GetKeyValueFromConnectionString("AccountName"), GetKeyValueFromConnectionString("AccountKey")));


            //Console.WriteLine($"{new BlobContainerClient(connectionString, containerName).Uri}?{sasToken}");
            Console.WriteLine($"{new BlobContainerClient(connectionString, containerName).Uri}?{sasToken}&restype=container&comp=list");

            /* Note : If you want to list the items inside container and view those details in a browser based on the generated SAS token
             * then two additional query parameters has to be appended to the token
             * the Query parameters are "restype=container&comp=list"
             */
        }


        public void GetAccountContainerAndBlobNameFromUrl(string Url)
        {
            var details = new BlobUriBuilder(new Uri(Url));

            Console.WriteLine($"Account Name {details.AccountName}");
            Console.WriteLine($"Container Name {details.BlobContainerName}");
            Console.WriteLine($"Blob Name {HttpUtility.UrlDecode(details.BlobName)}"); //When URL has spaces it will be encoded as %20, decode the value to get correct file name
        }

        public void ListAllBlobInCotainer(string containerName)
        {
            var blobs=new BlobContainerClient(connectionString, containerName).GetBlobs();
            foreach(var blob in blobs)
            {
                Console.WriteLine(blob.Name);
            }
        }

        public async Task CopyBlobFromOneContainerToAnother(string sourceUrl,string destinationUrl)
        {
            /* Note : If SAS token is there in URL then the connection string passed to BlobClient object will be skipped and SAS token will be considered for access permissions
             * The SAS token in source and destination url is useful if you are copying blob's across different accounts
             * If you have connection string then it is advisable to remove SAS token from URL, so that you will not end up in token expiry time issues.
             */
            var destBlobDetails = new BlobUriBuilder(new Uri(destinationUrl));
            var initiateCopy=new BlobClient(connectionString, destBlobDetails.BlobContainerName, destBlobDetails.BlobName).StartCopyFromUri(new Uri(sourceUrl)); //Start Copy queues copy operation to Azure Blob Storage/Emulator service and it happens in background. 
            var response = await initiateCopy.WaitForCompletionAsync(new TimeSpan(0, 0, 10), new System.Threading.CancellationToken()); //In-order to wait for copy function to complete call WaitForCompletionAsync with time interval to know the status. Here once in 10 seconds we are querying the storage service for copy status.


            if (response.GetRawResponse().Status == 200) //Status code 200 will be returned once copy is completed
                Console.WriteLine("Copy completed");
        }


        static void Main(string[] args)
        {

            var containerName = "learntocreatecontainer";
            var connectionString = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

            //Uncomment the methods which ever is necessary

            var blobStorageService = new BlobStorageService(connectionString);
            blobStorageService.CreateContainerIfDoesNotExists(containerName);
            //blobStorageService.DeleteContainerIfExists(containerName);
            //blobStorageService.AddNewOrUpdateAccessPoliciesToContainer(containerName);
            //blobStorageService.RemoveAccessPolicyIfExists(containerName, "Read_Write_Policy");
            //blobStorageService.SetPublicAccessLevel(containerName);
            //blobStorageService.RemovePublicAccessLevel(containerName);
            //blobStorageService.UploadBlob(containerName, "FirstUpload.txt", "C:\\Temp\\filetobeuploaded.txt");
            //blobStorageService.UploadBlob(containerName, "Folder1/FirstUpload.txt", "C:\\Temp\\filetobeuploaded.txt"); // Uploading file to a folder in container
            //blobStorageService.DeleteBlob(containerName, "Folder1/FirstUpload.txt"); // Folders will be automatically deleted if there is no blob in it
            blobStorageService.DownloadBlob(containerName, "Folder1/FirstUpload.txt", "c:\\Temp\\Downloaded_FirstUpload.txt");
            //blobStorageService.GetBlobProperties(containerName, "FirstUpload.txt");
            //blobStorageService.GetAdHocBlobSasToken(containerName, "FirstUpload.txt");
            //blobStorageService.GetAccessPolicyBasedSasToken(containerName, "FirstUpload.txt", "DemoAccessPolicy");
            //blobStorageService.GetAdHocContainerSasToken(containerName);
            //blobStorageService.GetAccessPolicyBasedContainerSasToken(containerName, "DemoAccessPolicy");
            //blobStorageService.GetAccountContainerAndBlobNameFromUrl("http://127.0.0.1:10000/devstoreaccount1/learntocreatecontainer/First%20Upload.txt");
            //blobStorageService.CopyBlobFromOneContainerToAnother("http://127.0.0.1:10000/devstoreaccount1/sourcecontainer/temp.txt", "http://127.0.0.1:10000/devstoreaccount1/destcontainer/temp.txt").GetAwaiter().GetResult();
            blobStorageService.ListAllBlobInCotainer(containerName);
            Console.ReadLine();
        }
    }
}
