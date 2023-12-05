using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using System.Security.Cryptography.X509Certificates;
using Mongo_revamp_agenet.RevampOper;
using ThirdParty.Json.LitJson;
using Mongo_revamp_agenet.MongoEntity;
using Serilog;
using System.Diagnostics;
using AnkurJfrogLibSQLAgent;

namespace Mongo_revamp_agenet
{
    public class Program
    {
        static void Main(string[] args)
        {
            //Logger the every Line For 
            Log.Logger = new LoggerConfiguration()
               .WriteTo.Console()
                 .WriteTo.File(
                    @"C:\Users\ankur\Desktop\Catchckankurresultruntime\log.txt",
                    rollingInterval: RollingInterval.Day, 
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
                    fileSizeLimitBytes: null,
                    retainedFileCountLimit: null,
                    shared: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1))
               .CreateLogger();
            var fillDataStopwatch = Stopwatch.StartNew();   

            string sqlConnectionString = "server=AVINEXSERVER6;database=CatCheckPro;Integrated Security=false;User ID=CCAdmin;Password=Catalyst1*;Trusted_Connection=No;";
            var res = RevampOperationAnkur.RevampSQLAgentAnkur(sqlConnectionString, "CC_Revamp_uspGetUMData", 3081, "5893209,5893210");
            IEnumerable<MongoRevampEntity> revampData = Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<MongoRevampEntity>>(res, new DateTimeToMillisecondsConverter());
            fillDataStopwatch.Stop();
            Log.Information("Data fill operation took: {ElapsedMilliseconds} ms", fillDataStopwatch.ElapsedMilliseconds);
           
            var pushTOMongoStopwatch = Stopwatch.StartNew();

            string mongoConnectionString = "mongodb://10.2.10.19:27017/";
            string mongoDatabaseName = "Um_test_cycle_sql_";
            string mongoCollectionName = "cycle_01";
            MongoClient mongoClient = new MongoClient(mongoConnectionString);
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(mongoDatabaseName);
            IMongoCollection<MongoRevampEntity> mongoCollection = mongoDatabase.GetCollection<MongoRevampEntity>(mongoCollectionName);
            mongoCollection.InsertMany(revampData);
            Log.Information("Data push to MongoDB operation took: {ElapsedMilliseconds} ms", pushTOMongoStopwatch.ElapsedMilliseconds);

            pushTOMongoStopwatch.Stop();
            
            
           

            Log.CloseAndFlush();

            Console.ReadLine(); 

        }
    }

            
        








            
             




            
        
    
}

