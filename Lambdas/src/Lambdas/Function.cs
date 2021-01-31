using System;
using System.Collections.Generic;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using System.Net;
using Newtonsoft.Json.Linq;
using Amazon.S3;
using System.Threading.Tasks;
using Amazon.S3.Model;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using System.Collections;
using System.Text.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Lambdas
{
    public class Function
    {
        /* 
        *  Sample input json body to submit for this Lambda
        *  
        *  {
             "anyjson":"somevalue"
           }
        */
        public object SimpleLambdaHandler(object input, ILambdaContext context)
        {
            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            try
            {
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                responseText += "SimpleLambda CDK Lambda call at " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                responseText += "with Environment variable=" + environment;

                //Reading the incoming request body to show we received it
                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
            }
            catch (Exception exc)
            {
                message+= "SimpleLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
                success = false;
            }

            //create the responseBody for the response
            string responseBody = "{\n";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }

        /** Sample request json
           * {
                  "anyjson":"somevalue"
             }
           */
        public object S3LambdaHandler(object input, ILambdaContext context)
        {
            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string s3ObjectsJson = "";

            try
            {
                responseText += "S3Lambda CDK Lambda call at " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                string bucketName = Environment.GetEnvironmentVariable("BUCKET");
                string region = Environment.GetEnvironmentVariable("REGION");
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                responseText += "Region:" + region;
                responseText += "S3 Bucket location:" + bucketName;

                S3ReportService s3Report = new S3ReportService();
                s3Report.generateReport(region, bucketName);
                responseText += "# Files in the bucket=" + s3Report.s3Objects.Count;
                responseText += "**S3 Report Log= " + s3Report.reportLog;
                s3ObjectsJson = "[";
                
                //prepare to return information on any s3 objects found
                for (int i=0;i<s3Report.s3Objects.Count;i++)
                {
                    S3Object currentS3Object = (S3Object)s3Report.s3Objects[i];
                    var s3ObjectJson = "{\n";
                    s3ObjectJson += "\"key\": \""+ currentS3Object.Key+ "\",\n";
                    s3ObjectJson += "\"bucketname\": \"" + currentS3Object.BucketName + "\",\n";
                    s3ObjectJson += "\"region\": \"" + region + "\",\n";
                    s3ObjectJson += "\"size\": \"" + currentS3Object.Size + "\",\n";
                    s3ObjectJson += "\"lastmodified\": \"" + currentS3Object.LastModified + "\"";
                    s3ObjectJson += "}";
                    if (i<s3Report.s3Objects.Count-1)
                    {
                        s3ObjectJson += ",";
                    }

                    s3ObjectsJson += s3ObjectJson +"\n";
                }
                s3ObjectsJson += "]";


                //request body
                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                DateTime endTime = DateTime.Now;
            }
            catch (Exception exc)
            {
                message += "S3LambdaHandler Exception:" + exc.Message + ":" + exc.StackTrace;
                success = false;
            }

            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"s3Objects\":" + s3ObjectsJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }
 

        /** Sample request json
         * {
                "email":"jeff.bezos@amazon.com",
         "firstName":"Jeff",
            "lastName":"Bezos"
                }
         */
        public object WriteDynamoDBLambdaHandler(object input, ILambdaContext context)
        {

            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string userObjectJson = "{}";

            try
            {
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                string tableName = Environment.GetEnvironmentVariable("TABLE");
                responseText = "WriteDynamoDB CDK Lambda " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                responseText += "DynamoDB Table:" + tableName;

                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                var requestBodyJson = JObject.Parse(requestBody);

                string email = requestBodyJson["email"].ToString();
                string firstName = requestBodyJson["firstName"].ToString();
                string lastName = requestBodyJson["lastName"].ToString();

                //Create the user object to save
                User user = new User();
                user.email = email;
                user.firstName = firstName;
                user.lastName = lastName;
                DynamoDBContextConfig config = new DynamoDBContextConfig()
                {
                    TableNamePrefix = environment + "-"
                };
                DynamoDBContext dynamoDbContext = new DynamoDBContext(new AmazonDynamoDBClient(), config);
                dynamoDbContext.SaveAsync(user);
                responseText += " Successfully saved User(" + user.email +","+firstName+","+lastName + ") to our DynamoDB table "+environment + "-User";
                userObjectJson = JsonSerializer.Serialize(user);
            }
            catch (Exception exc)
            {
                success = false;
                message += "WriteDynamoDBLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
            }


            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"user\":" + userObjectJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }

        /* 
  *  
  *  {
        "email":"jeff.bezos@amazon.com"
     }
  */
        public object ReadDynamoDBLambdaHandler(object input, ILambdaContext context)
        {

            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string userObjectJson = "{}";

            try
            {
                string dynamicText = "ReadDynamoDB CDK Lambda " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                string tableName = Environment.GetEnvironmentVariable("TABLE");

                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                var requestBodyJson = JObject.Parse(requestBody);

                string email = requestBodyJson["email"].ToString();

                responseText += "Searching for user(" + email+")";
                DynamoDbUserService dynamoDbUserService = new DynamoDbUserService();
                dynamoDbUserService.GetDynamoDbUser(environment, email).Wait();
                User user = dynamoDbUserService.user;
                responseText += "DynamoDBUserService Log:" + dynamoDbUserService.log + ".";
                if (user != null)
                {
                    userObjectJson = JsonSerializer.Serialize(user);
                    responseText += " Found the User:" + user.email + "," + user.firstName + "," + user.lastName;
                }
                else
                {
                    success = false;
                    userObjectJson = "{}";
                    responseText += " Did not find the user(" + email + ")";
                    message = "Did not find the user(" + email + ")";
                }
            }
            catch (Exception exc)
            {
                success = false;
                message += "ReadDynamoDBLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
            }


            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"user\":" + userObjectJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }
    }

    public class S3ReportService
    {
        private static AmazonS3Client s3Client = null;

        public string reportLog { get; set; }
        public ArrayList s3Objects = new ArrayList();

        public void generateReport(string region, string bucketName)
        {
            reportLog = "S3ReportService ReportLog region=" + region + " bucketName=" + bucketName+" ";
            if (s3Client == null)
            {
                s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(region));
            }
            this.ReadObjectDataAsync(region, bucketName).Wait();
        }
        public async Task ReadObjectDataAsync(string region, string bucketName)
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest
                {
                    BucketName = bucketName,
                };
                ListObjectsResponse response = await s3Client.ListObjectsAsync(request);
                int index = 0;
                if (response.S3Objects != null)
                {
                    foreach (S3Object o in response.S3Objects)
                    {
                        index++;
                        s3Objects.Add(o);
                     }
                 }
            }
            catch (Exception e)
            {
                reportLog += "S3ReportService.ReadObjectDataAsync Exception:" + e.Message + ":" + e.StackTrace;
            }
        }
    }

    public class DynamoDbUserService
    {
        private static AmazonDynamoDBClient dynamoDbClient = null;
        public User user { get; set; }
        public string log { get; set; }

        public async Task GetDynamoDbUser(string environment, string email)
        {
            if (dynamoDbClient == null)
            {
                dynamoDbClient = new AmazonDynamoDBClient();
            }

            DynamoDBContextConfig config = new DynamoDBContextConfig()
            {
                TableNamePrefix = environment + "-"
            };
            DynamoDBContext context = new DynamoDBContext(dynamoDbClient, config);
            log = "Looking for user(" + email + ") within dynamodb table " + environment + "-User.";
            user = await context.LoadAsync<User>(email);
            if (user != null)
            {
                log += "*** Found that user ***";
            }
            else
            {
                log += "*** Did not find that user";
            }
        }
    }

    [DynamoDBTable("User")]
    public class User
    {
        [DynamoDBHashKey]
        public string email { get; set; }
        [DynamoDBProperty("firstName")]
        public string firstName { get; set; }
        [DynamoDBProperty("lastName")]
        public string lastName { get; set; }
    }
}