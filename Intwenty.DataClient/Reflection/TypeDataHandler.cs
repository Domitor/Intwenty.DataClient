﻿using System;
using System.Data;
using Intwenty.DataClient.Model;
using System.Runtime.Caching;

namespace Intwenty.DataClient.Reflection
{

    static class TypeDataHandler
    {


        public static IntwentyDbTableDefinition GetDbTableDefinition<T>(string key)
        {
            var currenttype = typeof(T);
            var compositekey = (currenttype.Name + "_" + key).ToUpper();
            return GetTableInfoByTypeAndUsageInternal<T>(compositekey, currenttype);
        }

        public static IntwentyDbTableDefinition GetDbTableDefinition<T>()
        {
            var currenttype = typeof(T);
            var key = currenttype.Name.ToUpper();
            return GetTableInfoByTypeAndUsageInternal<T>(key, currenttype);
        }

        public static void AdjustColumnDefinitionToQueryResult(IntwentyDbTableDefinition t, IDataReader reader)
        {
            if (reader == null)
                return;

            foreach (var c in t.Columns)
                c.IsInQueryResult = false;

            for (int c = 0; c < t.Columns.Count; c++)
            {
                var col = t.Columns[c];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i).ToLower() == col.Name.ToLower())
                    {
                        col.IsInQueryResult = true;
                        col.Index = i;
                        break;
                    }
                }

            }


            t.Columns.RemoveAll(p => !p.IsInQueryResult);

        }


        private static IntwentyDbTableDefinition GetTableInfoByTypeAndUsageInternal<T>(string key, Type currenttype)
        {


            var cache = MemoryCache.Default;


            IntwentyDbTableDefinition result = cache.Get(key) as IntwentyDbTableDefinition;
            if (result != null)
            {
                 return result;
            }

            result = new IntwentyDbTableDefinition() { Id = key, Name = currenttype.Name };

            var tablename = currenttype.GetCustomAttributes(typeof(DbTableName), false);
            if (tablename != null && tablename.Length > 0)
                result.Name = ((DbTableName)tablename[0]).Name;

            var primarykey = currenttype.GetCustomAttributes(typeof(DbTablePrimaryKey), false);
            if (primarykey != null && primarykey.Length > 0)
                result.PrimaryKeyColumnNames = ((DbTablePrimaryKey)primarykey[0]).Columns;

            var indexes = currenttype.GetCustomAttributes(typeof(DbTableIndex), false);
            if (indexes != null && indexes.Length > 0)
            {
                var idxcnt = -1;
                foreach (var a in indexes)
                {
                    idxcnt++;
                    var idx = (DbTableIndex)a;
                    var tblindex = new IntwentyDbIndexDefinition() { Id = idx.Name, Name = idx.Name, ColumnNames = idx.Columns, IsUnique = idx.IsUnique, Index = idxcnt, TableName = result.Name };
                    result.Indexes.Add(tblindex);
                }
            }

            var order = -1;
            var memberproperties = currenttype.GetProperties();
            foreach (var property in memberproperties)
            {
                var membername = property.Name;
                var columnname = property.GetCustomAttributes(typeof(DbColumnName), false);
                if (columnname != null && columnname.Length > 0)
                    membername = ((DbColumnName)columnname[0]).Name;

                var column = new IntwentyDbColumnDefinition() { Id=membername,  Name = membername, Property = property };

                var autoinc = property.GetCustomAttributes(typeof(AutoIncrement), false);
                if (autoinc != null && autoinc.Length > 0)
                    column.IsAutoIncremental = true;

                var notnull = property.GetCustomAttributes(typeof(NotNull), false);
                if (notnull != null && notnull.Length > 0)
                    column.IsNullNotAllowed = true;

                var ignore = property.GetCustomAttributes(typeof(Ignore), false);
                if (ignore != null && ignore.Length > 0)
                    column.IsIgnore = true;

                if (property.PropertyType.IsArray)
                    column.IsIgnore = true;

                if (result.PrimaryKeyColumnNames.Length == 0 && membername.ToUpper() == "ID")
                {
                    column.IsPrimaryKeyColumn = true;
                    result.PrimaryKeyColumnNames = membername;
                }
                else
                {
                    if (result.PrimaryKeyColumnNamesList.Exists(p => p.ToUpper() == membername.ToUpper()))
                    {
                        column.IsPrimaryKeyColumn = true;
                        column.IsNullNotAllowed = true;
                    }
                }

                var typestring = property.PropertyType.FullName;

                if (typestring.ToUpper() == "SYSTEM.INT32" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.INT32")))
                    column.IsInt32 = true;
                else if (typestring.ToUpper() == "SYSTEM.BOOLEAN" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.BOOLEAN")))
                    column.IsBoolean = true;
                else if (typestring.ToUpper() == "SYSTEM.DECIMAL" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.DECIMAL")))
                    column.IsDecimal = true;
                else if (typestring.ToUpper() == "SYSTEM.SINGLE" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.SINGLE")))
                    column.IsSingle = true;
                else if (typestring.ToUpper() == "SYSTEM.DOUBLE" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.DOUBLE")))
                    column.IsDouble = true;
                else if (typestring.ToUpper() == "SYSTEM.DATETIME" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.DATETIME") && !typestring.ToUpper().Contains("SYSTEM.DATETIMEOFFSET")))
                    column.IsDateTime = true;
                else if (typestring.ToUpper() == "SYSTEM.DATETIMEOFFSET" || (typestring.ToUpper().Contains("NULLABLE") && typestring.ToUpper().Contains("SYSTEM.DATETIMEOFFSET")))
                    column.IsDateTimeOffset = true;
                else if (typestring.ToUpper() == "SYSTEM.STRING")
                    column.IsString = true;

                if (!column.IsIgnore)
                    order += 1;

                column.Index = order;
                result.Columns.Add(column);
               

            }

            cache.Add(key, result, DateTime.Now.AddYears(1));

            return result;


        }



    }
}
