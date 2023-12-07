using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnkurJfrogLibSQLAgent
{
    public class RevampOperationAnkur
    {
        /// <summary>
        /// @Ankur_Mall_exAdformIndia_use as a socket Programming 
        /// </summary>
        /// <param name="sqlConnectionString"></param>
        /// <param name="storeProcedureName"></param>
        /// <param name="CycleId"></param>
        /// <param name="rdids"></param>
        /// <returns></returns>
        public static string RevampSQLAgentAnkur(string sqlConnectionString, string storeProcedureName, int CycleId, string rdids)
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
