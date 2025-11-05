using System.Data;
using System;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;

public class MySqlHelper
{
    private readonly string _connectionString;

    public MySqlHelper(string connectionString)
    {
        _connectionString = connectionString;
    }

    private MySqlConnection GetConnection()
    {
        return new MySqlConnection(_connectionString);
    }

    // CREATE
    public int ExecuteNonQuery(string query, Dictionary<string, object> parameters = null)
    {
        using var conn = GetConnection();
        conn.Open();
        using var cmd = new MySqlCommand(query, conn);

        if (parameters != null)
        {
            foreach (var param in parameters)
                cmd.Parameters.AddWithValue(param.Key, param.Value);
        }

        return cmd.ExecuteNonQuery();
    }

    // READ
    public DataTable ExecuteQuery(string query, Dictionary<string, object> parameters = null)
    {
        using var conn = GetConnection();
        conn.Open();
        using var cmd = new MySqlCommand(query, conn);

        if (parameters != null)
        {
            foreach (var param in parameters)
                cmd.Parameters.AddWithValue(param.Key, param.Value);
        }

        using var adapter = new MySqlDataAdapter(cmd);
        var dt = new DataTable();
        adapter.Fill(dt);
        return dt;
    }

    // READ (Single value)
    public object ExecuteScalar(string query, Dictionary<string, object> parameters = null)
    {
        using var conn = GetConnection();
        conn.Open();
        using var cmd = new MySqlCommand(query, conn);

        if (parameters != null)
        {
            foreach (var param in parameters)
                cmd.Parameters.AddWithValue(param.Key, param.Value);
        }

        return cmd.ExecuteScalar();
    }
}
