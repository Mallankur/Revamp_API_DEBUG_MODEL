using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mongo_revamp_agenet.RevampOper
{
    public  class RevampOperation
    {
        /// <summary>
        /// @AnkurMall__Data IMPORT_ SQL_To Ankur_Mall any Sink_{cosmos/Mongo/AZUREBLOB_Any__)
        /// </summary>
        /// <param name="sqlConnectionString"></param>
        /// <param name="storeProcedureName"></param>
        /// <param name="CycleId"></param>
        /// <param name="rdids"></param>
        /// <returns></returns>
        public static string  RevampSQLAgent(string sqlConnectionString,string storeProcedureName,int CycleId , string rdids)
        {
          
           

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                SqlCommand sqlCommand = new SqlCommand(storeProcedureName, sqlConnection); 
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.Parameters.AddWithValue("@CycleId", CycleId); 
                sqlCommand.Parameters.AddWithValue("@RDIds", rdids); 




                SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);
                DataTable dataTable = new DataTable();
                adapter.Fill(dataTable);


                string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(dataTable);

                return jsonData;


     
            }
        }
    }
}
