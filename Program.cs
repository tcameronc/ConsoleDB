using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ConnectToCHDTestDB
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Begin Connecting...");

            var datasource = "localhost";// local sql server
            var database = "CHDTestDB"; // Dateabase name
            var username = "taylor"; // username
            var password = ""; // no password for simplicity
            
            string connString = @"Data Source=" + datasource + ";Initial Catalog="
                        + database + ";Trusted_Connection=true;User ID=" + username + ";Password=" + password;
            //full string using concat

            //create instanace of above connection
            SqlConnection conn = new SqlConnection(connString);

            
            //Try with catch required for DB in C# and Java

            try
            {
                Console.WriteLine("...Connecting...");

                conn.Open(); //opens above connection

                Console.WriteLine("...Success!");

                Console.WriteLine("Enter MemberID");
                string MemberID = Console.ReadLine(); //User inputs their member ID to only get matching records

                if (conn.State == ConnectionState.Open)
                {
                    StringBuilder strBuilder = new StringBuilder(); //string to place query into
                    
                    //feed entire DB query to SQL through C#. Another method or separating this in some way would be cleaner

                    strBuilder.Append("WITH CTE AS \r\n(SELECT Member.MemberID, Member.FirstName, Member.LastName, " +
                        "\r\nDiagnosis.DiagnosisDescription ,\r\nMemberDiagnosis.DiagnosisID,\r\nDiagnosisCategory.DiagnosisCategoryID ," +
                        "\r\nDiagnosisCategory.CategoryScore , \r\n[RN] = ROW_Number() OVER(PARTITION BY " +
                        "\r\nDiagnosisCategory.CategoryScore, MemberDiagnosis.DiagnosisID\r\nOrder BY DiagnosisCategory.DiagnosisCategoryID)" +
                        "\r\n\r\nFROM \r\n(Member INNER JOIN MemberDiagnosis ON Member.MemberID = MemberDiagnosis.MemberID" +
                        "\r\nINNER JOIN Diagnosis ON MemberDiagnosis.DiagnosisID = Diagnosis.DiagnosisID" +
                        "\r\nINNER JOIN DiagnosisCategoryMap ON DiagnosisCategoryMap.DiagnosisID = Diagnosis.DiagnosisID" +
                        "\r\nINNER JOIN DiagnosisCategory ON DiagnosisCategory.DiagnosisCategoryID = DiagnosisCategoryMap.DiagnosisCategoryID))" +
                        "\r\n\r\nSELECT CTE.MemberID, CTE.FirstName, CTE.LastName, \r\nCTE.DiagnosisDescription AS [MostSevereDiagnosisDescription]," +
                        "\r\nCTE.DiagnosisID AS [MostSevereDiagnosisID],\r\nCTE.DiagnosisCategoryID AS [CategoryID],\r\nCTE.CategoryScore AS [CategoryScore], " +
                        "\r\n\r\nCASE\r\n\tWHEN CTE.RN = 1 THEN '1'\r\n\tELSE '0'\r\nEND AS IsMostSevereDiagnosis\r\n\r\nFROM CTE WHERE MemberID =" 
                        + MemberID + "\r\nORDER BY CTE.MemberID ");
                    
                    //MemberID above allows user to specify the ID that gets returned

                    string sqlQuery = strBuilder.ToString();
                    using (SqlCommand command = new SqlCommand(sqlQuery, conn)) //query created above and connection
                    {
                        using (SqlDataReader reader = command.ExecuteReader()) //reads SQL data into console text output
                        { 
                            while (reader.Read())
                            {
                                Console.WriteLine(String.Format("MemberID|{0}| FirstName|{1}| " + //One Writelinee Statement
                                                                                                  //per table row
                                    "LastName|{2}| MostSevereDiagnosisID|{3}|" +                  //(Could be spaced more nicely)
                                    "MostSevereDiagnosisDescription|{4}| CategoryID|{5}| " +
                                    "CategoryDescription|{6}| IsMostSevereCategory|{7}|", 
                                reader[0], reader[1], reader[2], reader[3], 
                                reader[4], reader[5], reader[6], reader[7]));

                            }
                        }
                        command.ExecuteNonQuery(); //hopefully, query executes
                        Console.WriteLine("Query Executed."); //success!
                    }
                }

                
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message); //fail message in casae of mistake
            }

            Console.Read(); // freezes console output
        }
    }
}