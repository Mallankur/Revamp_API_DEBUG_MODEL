using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace Mongo_revamp_agenet
{
    public class Program
    {
        static void Main(string[] args)
        {
            string sqlConnectionString = "server=AVINEXSERVER6;database=CatCheckPro;Integrated Security=false;User ID=CCAdmin;Password=Catalyst1*;Trusted_Connection=No;";

            string mongoConnectionString = "mongodb://10.2.10.5:27017/";
            string mongoDatabaseName = "Um_test_cycle_sql_to_mongodb";
            string mongoCollectionName = "cycle_01_sql_to_mongo";


            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                SqlCommand sqlCommand = new SqlCommand("CC_Revamp_uspGetUMData", sqlConnection);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.Parameters.AddWithValue("@CycleId", 2694);
                sqlCommand.Parameters.AddWithValue("@RDIds", "4216921,4216922");




                SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);
                DataTable dataTable = new DataTable();
                adapter.Fill(dataTable);


                MongoClient mongoClient = new MongoClient(mongoConnectionString);
                IMongoDatabase mongoDatabase = mongoClient.GetDatabase(mongoDatabaseName);
                IMongoCollection<BsonDocument> mongoCollection = mongoDatabase.GetCollection<BsonDocument>(mongoCollectionName);


                foreach (DataRow row in dataTable.Rows)
                {
                    var document = new BsonDocument
                    {
                        { "_id", Guid.NewGuid() },
                        { "Sapunitno", row["Sapunitno"] as int? },
                        { "Cycleno", row["Cycleno"] as long? },
                        { "CycleId", row["CycleId"] as long? },
                        { "DOS", row["DOS"] as int? },
                        { "DOSDate", row["DOSDate"] as long? },
                        { "CC_Fields_Defs_Id", row["CC_Fields_Defs_Id"] as int? },
                        { "CSISValue", row["CSISValue"] as double? },
                        { "ImputedValue", row["ImputedValue"] as double? },
                        { "ImputedValueMetric", row["ImputedValueMetric"] as double? },
                        { "ImputedValueImperial ", row["ImputedValueImperial"] as double? },
                        { "CleansedValue", row["CleansedValue"] as double? },
                        { "ValueMetric", row["ValueMetric"] as double? },
                        { "ImportedValue ", row["ImportedValue"] as double? },
                        { "CSISDataTypeId", row["CSISDataTypeId"] as long? },
                        { " CSISTestRunId ", row["CSISTestRunId"] as long? },
                        { "CSISPredictionId ", row["CSISPredictionId"] as long? },
                        { "EOMobileLabId", row["EOMobileLabId"] as long? },
                        { "ReportDataEntityId ", row["ReportDataEntityId"] as long? },
                        { "IgnoreError ", row["IgnoreError"] as bool? },
                        { "ApplicationId", row["ApplicationId"] as long? },
                        { "Mode ", row["Mode"] as int? }


                    };
                    mongoCollection.InsertOne(document);
                }
                Console.WriteLine("Data transfer from SQL to MongoDB completed.");
            }
        }
    }
}

