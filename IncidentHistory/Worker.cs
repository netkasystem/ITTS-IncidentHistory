using Microsoft.VisualBasic;
using MySql.Data.MySqlClient;
using Mysqlx.Crud;
using System.Data;
using System.Text.RegularExpressions;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace IncidentHistory
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private string ConnectionString;
        private MySqlHelper mySqlHelper;
        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            ConnectionString = Environment.GetEnvironmentVariable("NSDXConnectionString");
            mySqlHelper = new MySqlHelper(ConnectionString);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TimeSpan TDlay = TimeSpan.FromDays(1);
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                    string LsCaseIdStr = "";
                    var sql = "SELECT " +
                        " TIMESTAMPDIFF(DAY , closed_date , CURDATE()) closed_to_now,DATE(closed_date)closed_date " +
                        " from incident " +
                        " where case_status_id = 16 " +
                        " and closed_date != '0000-00-00 00:00:00' " +
                        " and closed_date<DATE_SUB(CURDATE(), INTERVAL 3 MONTH) " +
                        " GROUP BY DATE(closed_date); ";
                    var LsDay = mySqlHelper.ExecuteQuery(sql).AsEnumerable().Select(r => r.Field<long>("closed_to_now")).ToList();
                    int i = 0;

                    foreach (var item in LsDay)
                    {
                        if (i < 10 && uint.TryParse(item.ToString(), out uint val))
                        {
                            MoveIncidentToHistory(Convert.ToUInt32(item));
                            i++;
                        }
                    }
                    if(LsDay.Count > 1) TDlay = TimeSpan.FromMinutes(5);
                }
                await Task.Delay(TDlay, stoppingToken);
            }
        }


        public bool MoveIncidentToHistory(uint DayInterval)
        {
            string LsCaseIdStr = "";
            var sql = "SELECT id as Id FROM incident WHERE case_status_id = 16  " +
                 "AND closed_date IS NOT NULL " +
                 "AND closed_date <> '0000-00-00 00:00:00' " +
                 "AND closed_date < CURDATE() - INTERVAL {0} DAY " +
                 "AND closed_date >= CURDATE() - INTERVAL {1} DAY";
            sql = string.Format(sql, DayInterval, DayInterval + 1);
            var LsCaseId = mySqlHelper.ExecuteQuery(sql);

            LsCaseIdStr = string.Join(",", LsCaseId.AsEnumerable().Select(r => r.Field<uint>("Id")));


            if (LsCaseIdStr != "")
            {
                //get case_attachment_id
                string LsAttachmentIdStr = "";
                sql = "SELECT attachment_id as AttachmentId FROM case_attachment " +
                "WHERE module_id = 1 AND case_id IN ({0})";
                sql = string.Format(sql, LsCaseIdStr);
                var LsAttachmentId = mySqlHelper.ExecuteQuery(sql);   
                LsAttachmentIdStr = string.Join(",", LsAttachmentId.AsEnumerable().Select(x => x.Field<uint>("AttachmentId")).ToArray());


                // move incident
                string sql_insert = "INSERT INTO incident_history SELECT * FROM incident WHERE case_status_id = 16  " +
                "AND closed_date IS NOT NULL " +
                "AND closed_date <> '0000-00-00 00:00:00' " +
                "AND closed_date < CURDATE() - INTERVAL {0} DAY";
                sql_insert = string.Format(sql_insert, DayInterval);
                string insert = mySqlHelper.ExecuteNonQuery(sql_insert).ToString() ?? "";

                // delete incident
                string sql_delete = "DELETE FROM incident WHERE case_status_id = 16  " +
                "AND closed_date IS NOT NULL " +
                "AND closed_date <> '0000-00-00 00:00:00' " +
                "AND closed_date < CURDATE() - INTERVAL {0} DAY";
                sql_delete = string.Format(sql_delete, DayInterval);
                string detele = mySqlHelper.ExecuteNonQuery(sql_delete).ToString() ?? "";

                // move case_log
                sql_insert = "INSERT INTO case_log_history SELECT * FROM case_log WHERE case_id IN ({0})";
                sql_insert = string.Format(sql_insert, LsCaseIdStr);
                insert = mySqlHelper.ExecuteNonQuery(sql_insert).ToString() ?? "";

                // delete case_log
                sql_delete = "DELETE FROM case_log WHERE case_id IN ({0})";
                sql_delete = string.Format(sql_delete, LsCaseIdStr);
                detele = mySqlHelper.ExecuteNonQuery(sql_delete).ToString() ?? "";


                if (LsAttachmentIdStr != "")
                {
                    // move case_attachment
                    sql_insert = "INSERT INTO case_attachment_history SELECT * FROM case_attachment " +
                        "WHERE module_id = 1 AND case_id IN ({0})";
                    sql_insert = string.Format(sql_insert, LsCaseIdStr);
                    insert = mySqlHelper.ExecuteNonQuery(sql_insert).ToString() ?? "";

                    // move file
                    sql_insert = "INSERT INTO nsdx_file_attatchment_history SELECT * FROM nsdx_file_attatchment " +
                        " WHERE attatchment_id IN({0})";
                    sql_insert = string.Format(sql_insert, LsAttachmentIdStr);
                    insert = mySqlHelper.ExecuteNonQuery(sql_insert).ToString() ?? "";

                    // delete file
                    sql_delete = "DELETE FROM nsdx_file_attatchment " +
                        " WHERE attatchment_id IN({0})";
                    sql_delete = string.Format(sql_delete, LsAttachmentIdStr);
                    detele = mySqlHelper.ExecuteNonQuery(sql_delete).ToString() ?? "";

                    // delete case_attachment
                    sql_delete = "DELETE FROM case_attachment " +
                        "WHERE module_id = 1 AND case_id IN ({0})";
                    sql_delete = string.Format(sql_delete, LsCaseIdStr);
                    detele = mySqlHelper.ExecuteNonQuery(sql_delete).ToString() ?? "";
                }

            }
            return true;
        }

    }
}
