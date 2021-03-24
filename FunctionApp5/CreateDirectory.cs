using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using Azure;
using System.Diagnostics;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Reflection;

namespace FunctionApp5
{
    public class CreateDirectory
    {
        // create section
        string TerminateCreate;
        string CreateQueue;
        string CreateSasToken;
        string createFileSystemName;
        Uri CreateServiceUri;
        string ServiceBusConnection;
        static QueueClient _queueClient;
        private readonly TelemetryClient telemetryClient;
        private readonly IConfiguration _configuration;

        // create section
        static DataLakeServiceClient createServiceClient;
        static DataLakeFileSystemClient createFileSystemClient;

        public CreateDirectory(IConfiguration configuration)//, TelemetryConfiguration configuration)
        {
            telemetryClient = new TelemetryClient();
            telemetryClient.InstrumentationKey = configuration["APPINSIGHTS_INSTRUMENTATIONKEY"];

            _configuration = configuration;
            // create section
            TerminateCreate = configuration["TerminateCreate"];
            createFileSystemName = configuration["createFileSystemName"];
            CreateQueue = configuration["createQueue"];
            CreateSasToken = configuration["createSasToken"];
            CreateServiceUri = new Uri(configuration["createServiceUri"]);
            ServiceBusConnection = configuration["servicebusconnection"];

            if (_queueClient == null)
            {
                var csb = new ServiceBusConnectionStringBuilder(ServiceBusConnection);
                csb.EntityPath = CreateQueue;
                _queueClient = new QueueClient(csb);
            }
            if (createServiceClient == null)
            {
                createServiceClient = new DataLakeServiceClient(CreateServiceUri, new AzureSasCredential(CreateSasToken));
            }

            if (createFileSystemClient == null)
            {
                createFileSystemClient = createServiceClient.GetFileSystemClient(createFileSystemName);
            }
        }

        [FunctionName("CreateDirectory")]
        public async Task Run([ServiceBusTrigger("datalake-create", Connection = "servicebusconnection")] Message message, ILogger log)
        {
            //telemetryClient.TrackTrace($"CreateDirectory: Message {message.MessageId} dequeued. Attempt {message.SystemProperties.DeliveryCount}");
            // amended in Github.com
            // parse incoming CreateRequest message
            string payload = System.Text.Encoding.UTF8.GetString(message.Body);
            CreateRequest request = JsonConvert.DeserializeObject<CreateRequest>(payload);
            log.LogInformation($"CreateDirectory: path:{request.Path} depth:{request.CurrentDepth} numDir:{request.NumberOfDirectories} numFiles:{request.NumberOfFiles} createDir:{request.CreateDirectories} CreateFiles:{request.CreateFiles}");
            telemetryClient.TrackEvent($"CreateDirectory called {request.DirectoryPattern}",
                    new Dictionary<string, string> { { "path", request.Path }, { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() } });

            // using path, set where we are in terms of the current directory
            var directoryClient = createFileSystemClient.GetDirectoryClient(request.Path);

            // we are in a directory, so create the file for this directory
            if (request.CreateFiles == true)
            {
                for (int i = 0; i < request.NumberOfFiles; i++)
                {
                    var newFile = string.Format("{0}{1}.txt", request.FilePattern, i);
                    DataLakeFileClient fileClient = directoryClient.GetFileClient(newFile);
                    // test if this is another run and file exists already
                    if (await fileClient.ExistsAsync() == false)
                    {
                        var binDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location); // get path of test file
                        var rootDirectory = Path.GetFullPath(Path.Combine(binDirectory, ".."));
                        FileStream fileStream = File.OpenRead(rootDirectory + "/file-to-upload.txt");
                        var response = await fileClient.UploadAsync(fileStream, true);
                        if (response.GetRawResponse().Status == 200)
                        {
                            telemetryClient.TrackEvent($"File created {request.FilePattern}",
                                new Dictionary<string, string> { { "path", request.Path },
                            { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() },
                            { "response", response.ToString() } });
                        }
                        if (request.CreateAcls == true)
                        {
                            // set ACLs for the file
                            IList<PathAccessControlItem> accessControlList = PathAccessControlExtensions.ParseAccessControlList("user::rwx,group::r-x,other::rw-");
                            await fileClient.SetAccessControlListAsync(accessControlList);
                            telemetryClient.TrackEvent($"File ACL created {request.FilePattern}");
                        }
                    }
                    else
                    {
                        telemetryClient.TrackEvent($"File exists {request.FilePattern}",
                            new Dictionary<string, string> { { "path", request.Path },
                        { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() } });
                    }
                }
            }

            // increment the depth, or it will never stop
            request.CurrentDepth++;
            // if we are at the max path or I see the terminate flag, do nothing - your work here is complete
            if (request.CurrentDepth >= request.MaxDepth || TerminateCreate == "1")
            {
                log.LogInformation($"CreateLakeFolder: Message {message.MessageId} dequeued.  No more to do");
                telemetryClient.TrackEvent("Nothing to do");
                return;
            }

            if (request.CreateDirectories == true)
            {
                // create a number of directories 
                for (int i = 0; i < request.NumberOfDirectories; i++)
                {
                    // create directory
                    var newDir = string.Format("{0}{1}", request.DirectoryPattern, i);
                    var response = await directoryClient.CreateSubDirectoryAsync(newDir);
                    telemetryClient.TrackEvent($"Directory created {request.DirectoryPattern}",
                        new Dictionary<string, string> { { "path", request.Path },
                        { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() },
                        {"response", response.ToString() } });

                    if (request.CreateAcls == true)
                    {
                        // set ACL for the directory
                        IList<PathAccessControlItem> accessControlList = PathAccessControlExtensions.ParseAccessControlList("user::rwx,group::r-x,other::rw-");
                        await directoryClient.SetAccessControlListAsync(accessControlList);
                        telemetryClient.TrackEvent($"Dir ACL created {request.FilePattern}");
                    }
                }

                // create service bus messages for sub directories
                var requestPath = request.Path;
                for (int i = 0; i < request.NumberOfDirectories; i++)
                {
                    var newDir = string.Format("{0}{1}", request.DirectoryPattern, i);
                    // send a service bus message with this new path
                    var childRequest = request;
                    childRequest.Path = requestPath + "/" + newDir; // new path
                    string data = JsonConvert.SerializeObject(childRequest);
                    Message newMessage = new Message(Encoding.UTF8.GetBytes(data));
                    await _queueClient.SendAsync(newMessage);
                    telemetryClient.TrackEvent($"Message sent {request.DirectoryPattern}",
                        new Dictionary<string, string> { { "path", request.Path }, { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() } });
                }
            }
            telemetryClient.TrackEvent($"CreateDirectory ended {request.DirectoryPattern}",
                   new Dictionary<string, string> { { "path", request.Path }, { "deliveryCount", message.SystemProperties.DeliveryCount.ToString() } });
        }
    }
}
