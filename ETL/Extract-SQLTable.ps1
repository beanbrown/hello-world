Param (
    [int]$Run,
    [string]$Environment, # PROD, UA, or TEST
    [string]$DestinationTable,
    [string]$SchemaName
)
###
### Extract
###
### Replace linked server query to views and use .Net SqlBulkClient to extract from SQL to SQL
###
### -Winter 8/2016
###	Born
###

###
### INIT
###
Import-Module NBBPS -PassThru -Force
[void][Reflection.Assembly]::LoadWithPartialName("System.Data")
[void][Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient")

if (($DestinationTable -eq '') -and ($SchemaName -eq '')) {
    "At least 1 filter is required (-DestinationTable or -SchemaName)."
    exit 1; # bonk
}

$ErrorActionPreference = "SilentlyContinue"
if ($Run -eq $nul) { $Run = -1 }
$Global:Run = $Run
$BatchSize = 50000
$DBNull = [System.DBNull]::Value
$elapsed = [System.Diagnostics.Stopwatch]::StartNew()  

if (-Not $Environment) { 
    # can we figure it out based on computer name?
    "Environment param not provided."
    if ($env:COMPUTERNAME -match "BI(EW|FS)DB(?<env>.)\d\d") {
        "Attempting to find env from servername"
        switch ($matches['env']) {
            P { $env = "PROD"; break }
            U { $env = "UA"; break }
                
            default { $env = "TEST"; break; }
        }
    } else {
        "Could not determine env from servername, using default"
        $env = "TEST"
    }
}
elseif ($Environment -match "(PROD|UA|TEST)") { 
    "Valid env passed as param"
    $env = $Environment 
}
else { 
    "Invalid environment: $($Environment)"
    exit 1
}
"Using Environment: $env"
switch ($env) {
    PROD { $sqlsrvr = "biewlip01"; $database = "ETL"; $q = @{Query="SQLQuery";Server="SourceServer";Database="SourceDatabase"}; break; }
    UA   { $sqlsrvr = "biewliu01"; $database = "ETL"; $q = @{Query="SQLQuery_UA";Server="SourceServer_UA";Database="SourceDatabase_UA"}; break; }
    TEST { $sqlsrvr = "bifsdbt01"; $database = "ETL"; $q = @{Query="SQLQuery_TEST";Server="SourceServer_TEST";Database="SourceDatabase_TEST"}; break; }
}
$cs = "server=$sqlsrvr;database=$database;trusted_connection=true;"
$DestinationCS = "server=$sqlsrvr;database=Stage;trusted_connection=true;"

###################################################################################################
##
## Logging
##
Function AddLogStep {
    param(
        $tbl,
        $rows,
        $comment,
        $seconds
    )

    # Save to ETL log
    try {
        $etlsql = New-Object System.Data.SQLClient.SQLConnection
        $etlsql.ConnectionString = $cs
        $etlsql.Open()
        $cmd = $etlsql.CreateCommand()
        $cmd.CommandText = "exec etl.etl.LogStep N'Sales', '$Global:Run', N'$($tbl)', N'$($comment)', 5, N'Stage-VIPData.ps1', '$seconds', '$rows'"            
        $cmd.ExecuteNonQuery() | Out-Null
    }
    catch {
        $_.Exception.Message
        "exec etl.etl.LogStep N'Sales', '$Global:Run', N'$($tbl)', N'$($comment)', 5, N'Stage-VIPData.ps1', '$seconds', '$rows'"
    }
    finally {
        $etlsql.Close | Out-Null
    }
}

###
### MAIN
###
$sql = "
    select SO=1, TB=DestinationTable, SC=SchemaName, QY=$($q.Query), SS=$($q.Server), DB=$($q.Database), ID=QueryID, BS=BusinessSponsor, BR=BusinessRequirement, BO=BusinessOwner, IO=ITOwner
        from etl.ExtractQueries x 
            join etl.ExtractSources s on s.SourceID = x.SourceID 
            join etl.ExtractSchemas c on c.SchemaID = 2 -- VIP_FTC
        where DestinationTable = CASE '$($DestinationTable)' WHEN '' THEN DestinationTable ELSE '$($DestinationTable)' END 
            and SchemaName = CASE '$($SchemaName)' WHEN '' THEN SchemaName ELSE '$($SchemaName)' END 
"
# $dep = "
#     WITH dep AS (
# 	    SELECT d.QueryID, d.DependantID
# 	    from etl.ExtractQueries x 
# 		    join etl.ExtractSources s on s.SourceID = x.SourceID 
# 		    join etl.ExtractSchemas c on c.SchemaID = x.CompanyID
# 		    JOIN etl.ExtractDependancies d ON d.QueryID = x.QueryID
# 	    where DestinationTable = CASE '$($DestinationTable)' WHEN '' THEN DestinationTable ELSE '$($DestinationTable)' END 
#             and SchemaName = CASE '$($SchemaName)' WHEN '' THEN SchemaName ELSE '$($SchemaName)' END
#             and ( 
#                 AutoProcess = 1
#                 and DATEDIFF(minute, d.LastUpdate, GETDATE()) > d.DecayTime
#             )
#     )
#     select SO=0, TB=DestinationTable, SC=SchemaName, QY=$($q.Query), SS=$($q.Server), DB=$($q.Database), ID=x.QueryID, BS=x.BusinessSponsor, BR=x.BusinessRequirement, BO=x.BusinessOwner, IO=x.ITOwner
#     from etl.ExtractQueries x 
#         join etl.ExtractSources s on s.SourceID = x.SourceID 
#         join etl.ExtractSchemas c on c.SchemaID = x.SchemaID
# 	    JOIN dep d ON d.DependantID = x.QueryID
# "

$SQLTypes = @{
    'boolean'    = '[BIT]';
    'byte'       = '[NVARCHAR]';
    'char'       = '[NCHAR]';
    'datetime'   = '[DATETIME]';
    'decimal'    = '[DECIMAL](32, 16)';
    'double'     = '[DECIMAL](32, 16)';
    'guid'       = '[UNIQUEIDENTIFIER]'
    'int16'      = '[SMALLINT]';
    'int32'      = '[INT]';
    'int64'      = '[BIGINT]';
    'sbyte'      = '[NVARCHAR]';
    'single'     = '[REAL]';
    'string'     = '[NVARCHAR]';
    'timespan'   = '[TIME]';
}

AddLogStep "" 0 "Begin extracting to Source $($SourceCS) from $($DestinationCS), DT:$DestinationTable, SC:$SchemaName" 0

# Tables
$Tables = Invoke-SQL $sql $cs 0 '' 1 
# $Dependancies = Invoke-SQL $dep $cs 0 '' 1 
# $Dependancies | % { $Tables += $_ }
$Tables | Sort SO | % {

    ## $_.TB

    # If dependancies are null, skip to tables
    If ($_.ItemArray.Count -ne 0) {
        Try {
            $TableName = $_.TB
            $SchemaName = $_.SC
            $SourceQuery = $_.QY            
            $IsDependancy = @('Dependant Table', 'Table')[$_.SO]
            $QueryID = $_.ID;
            $ExtendedProperties = @{
                "ITOwner" = $_.IO;
                "BusinessSponsor" = $_.BS;
                "BusinessOwner" = $_.BO;
                "BusinessRequirement" = $_.BR;
            }
        
            $Elapsed = [System.Diagnostics.Stopwatch]::StartNew()  

            # Get the source table
            # $DataTable = Invoke-SQL $($_.QY) $RemoteCS 0 '' 1 ## This will load into memory; need to stream from IDataReader into SqlBulkCopy
            $SourceCS = "Server=$($_.SS);Database=$($_.DB);Trusted_Connection=true;Connection Timeout=600;"
            $SourceConn = New-Object System.Data.SqlClient.SqlConnection($SourceCS)
            $SourceCommand = New-Object System.Data.SqlClient.SqlCommand($SourceQuery, $SourceConn)
            $SourceCommand.CommandTimeout = 0
            $SourceConn.Open()
            [System.Data.SqlClient.SqlDataReader] $Reader = $SourceCommand.ExecuteReader()
            $DataTable = $Reader.GetSchemaTable()

            AddLogStep "[$($SchemaName)].[$($TableName)]" 0 "Starting extract into $($SourceCS)" 0

            # Does destination table exist?
            $DestinationConn = New-Object System.Data.SqlClient.SqlConnection($DestinationCS)
            $DestinationCommand = New-Object System.Data.SqlClient.SqlCommand("SELECT OBJECT_ID(N'[$($SchemaName)].[$($TableName)]')", $DestinationConn)
            $DestinationCommand.CommandTimeout = 0
            $DestinationConn.Open()
            $DestinationExists = $DestinationCommand.ExecuteScalar()

            if ($DestinationExists -eq $DBNull) {
                $DateColumns = ','
                $DataTable.Rows | ? { $_.ColumnName -match "_DT$" } | % {
                    $DateColumns += "$($_.ColumnName.Replace("_DT", "_DD")) as convert(DATE, $($_.ColumnName)),"
                    $DateColumns += "$($_.ColumnName.Replace("_DT", "_DK")) as convert(INT, convert(varchar(8), $($_.ColumnName), 112)),"
                    $DateColumns += "$($_.ColumnName.Replace("_DT", "_TT")) as convert(TIME, $($_.ColumnName)),"
                    $DateColumns += "$($_.ColumnName.Replace("_DT", "_TK")) as convert(INT, replace(convert(varchar(8), $($_.ColumnName), 108), ':', '')),"
                }
                
                $KeyHash1 = ($DataTable.Rows | ? { $_.ColumnName -match "_BK$" -and $_.NumericPrecision -eq 255 -and $_.DataTypeName -notmatch '(bit|uniqueidentifier)'}).ColumnName -join ", N'')+nchar(9)+isnull("
                $KeyHash1 = @('', "isnull($($KeyHash1), N'')")[$KeyHash1 -ne '']
                $KeyHash2 = ($DataTable.Rows | ? { $_.ColumnName -match "_BK$" -and $_.NumericPrecision -ne 255 -and $_.DataTypeName -notmatch '(bit|uniqueidentifier)'}).ColumnName -join ", 0))+nchar(9)+convert(nvarchar(100), isnull("
                $KeyHash2 = @('', "convert(nvarchar(100), isnull($($KeyHash2), 0))")[$KeyHash2 -ne '']
                $KeyHash3 = ($DataTable.Rows | ? { $_.ColumnName -match "_BK$" -and $_.DataTypeName -match 'uniqueidentifier'}).ColumnName -join ", '00000000-0000-0000-0000-000000000000'))+nchar(9)+convert(nvarchar(100), isnull("
                $KeyHash3 = @('', "convert(nvarchar(100), isnull($($KeyHash3), '00000000-0000-0000-0000-000000000000'))")[$KeyHash3 -ne '']
                $KeyHash4 = ($DataTable.Rows | ? { $_.ColumnName -match "_BK$" -and $_.DataTypeName -match 'bit'}).ColumnName -join "), '0'))+nchar(9)+convert(nvarchar(100), isnull(convert(int, "
                $KeyHash4 = @('', "convert(nvarchar(100), isnull(convert(int, $($KeyHash4)), '0'))")[$KeyHash4 -ne '']
                $KeyHash = "_KeyHash as isnull(convert(varbinary(20), hashbytes('SHA1', $((@($KeyHash1, $KeyHash2, $KeyHash3, $KeyHash4) -join '+').Trim('+')))), 0xDA39A3EE5E6B4B0D3255BFEF95601890AFD80709),"

                $ValHash1 = ($DataTable.Rows | ? { $_.ColumnName -notmatch "_BK$" -and $_.NumericPrecision -eq 255 -and $_.DataTypeName -notmatch '(bit|uniqueidentifier)'}).ColumnName -join ", N'')+nchar(9)+isnull("
                $ValHash1 = @('', "isnull($($ValHash1), N'')")[$ValHash1 -ne '']
                $ValHash2 = ($DataTable.Rows | ? { $_.ColumnName -notmatch "_BK$" -and $_.NumericPrecision -ne 255 -and $_.DataTypeName -notmatch '(bit|uniqueidentifier)'}).ColumnName -join ", 0))+nchar(9)+convert(nvarchar(100), isnull("
                $ValHash2 = @('', "convert(nvarchar(100), isnull($($ValHash2), 0))")[$ValHash2 -ne '']
                $ValHash3 = ($DataTable.Rows | ? { $_.ColumnName -notmatch "_BK$" -and $_.DataTypeName -match 'uniqueidentifier'}).ColumnName -join ", '00000000-0000-0000-0000-000000000000'))+nchar(9)+convert(nvarchar(100), isnull("
                $ValHash3 = @('', "convert(nvarchar(100), isnull($($ValHash3), '00000000-0000-0000-0000-000000000000'))")[$ValHash3 -ne '']
                $ValHash4 = ($DataTable.Rows | ? { $_.ColumnName -notmatch "_BK$" -and $_.DataTypeName -match 'bit'}).ColumnName -join "), '0'))+nchar(9)+convert(nvarchar(100), isnull(convert(int, "
                $ValHash4 = @('', "convert(nvarchar(100), isnull(convert(int, $($ValHash4)), '0'))")[$ValHash4 -ne '']
                $ValHash = "_ValHash as isnull(convert(varbinary(20), hashbytes('SHA1', $((@($ValHash1, $ValHash2, $ValHash3, $ValHash4) -join '+').Trim('+')))), 0xDA39A3EE5E6B4B0D3255BFEF95601890AFD80709),"

                $LastUpdate = "_LastUpdate DATETIME DEFAULT GETDATE()" 

                $CreateTableSQL = "
                    create table [$($SchemaName)].[$($TableName)] (
                        $((($DataTable.Rows | % { "[$($_.ColumnName)] $($SQLTypes."$($_.DataType.Name)")$(@('', "($($_.ColumnSize))")[($_.NumericPrecision -eq 255) -and -not ($_.DataTypeName -match '(date|bit|uniqueidentifier)')]) $(@('NOT NULL', 'NULL')[$_.AllowDBNull])" }) -Join ",`n") -replace "\(\)", '')
                        $DateColumns
                        $KeyHash
                        $ValHash
                        $LastUpdate
                    )"
                
                #Invoke-SQL $CreateTableSQL $DestinationCS 0 '' 0
                $CreateTableCommand = New-Object System.Data.SqlClient.SqlCommand($CreateTableSQL, $DestinationConn)
                $CreateTableCommand.ExecuteNonQuery() | Out-Null

                # So it's not a HEAP
                $CreateIndexSQL = "create nonclustered index nci_$($SchemaName)_$($TableName) on [$($SchemaName)].[$($TableName)] ([_KeyHash]) --include ([_ValHash])"
                $CreateIndexCommand = New-Object System.Data.SqlClient.SqlCommand($CreateIndexSQL, $DestinationConn)
                $CreateIndexCommand.ExecuteNonQuery() | Out-Null

                # Extended properties
                $ExtendedProperties.Keys | % {                        
                    $ExtendedPropertySQL = "EXEC sp_addextendedproperty 
	                    @name='$($_)', @value='$($ExtendedProperties.$_)',
	                    @level0type = N'SCHEMA', @level0name = '$($SchemaName)', 
	                    @level1type = N'TABLE', @level1name = '$($TableName)'"
                    $ExtendedPropertyCommand = New-Object System.Data.SqlClient.SqlCommand($ExtendedPropertySQL, $DestinationConn)
                    $ExtendedPropertyCommand.ExecuteNonQuery() | Out-Null
                }
            }

            # Bulk insert into destincation table
            $BulkTrans = $DstConn.BeginTransaction()
            $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestinationCS, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock)
            $BulkCopy.DestinationTableName = "[$($SchemaName)].[$($TableName)]"
            $BulkCopy.BulkCopyTimeout = 0
            $BulkCopy.BatchSize = $BatchSize
            $BulkCopy.EnableStreaming = $True
            $DataTable.Rows | % { [Void]$BulkCopy.ColumnMappings.Add($_.ColumnName, $_.ColumnName) }

            $TruncateCommand = New-Object System.Data.SqlClient.SqlCommand("truncate table [$($SchemaName)].[$($TableName)]", $DestinationConn)
            $TruncateCommand.ExecuteNonQuery() | Out-Null

            $BulkCopy.WriteToServer($Reader)

            $BulkTrans.Commit()

            # Update Query Run Time
            $RuntimeSQL = "update etl.etl.ExtractDependancies set LastUpdate = GetDate() where "
            if ($IsDependancy -eq 'Dependant Table') { $RuntimeSQL += "DependantID = $QueryID" } 
            else                                     { $RuntimeSQL += "QueryID = $QueryID" }
            $RuntimeConn = New-Object System.Data.SqlClient.SqlConnection($cs)
            $RuntimeCommand = New-Object System.Data.SqlClient.SqlCommand($RuntimeSQL, $RuntimeConn)
            $RuntimeCommand.CommandTimeout = 0
            $RuntimeConn.Open()
            $RuntimeCommand.ExecuteNonQuery() | Out-Null

            # Transform and Load
            $TransformLoadSQL = "
                exec etl.etl.Transform @SCHEMA = N'$SchemaName', @PREFIX = '$TableName', @Run = '$Run'
                exec etl.etl.Load @SCHEMA = N'$SchemaName', @PREFIX = '$TableName', @Run = '$Run'
            "
            $TransformLoadCommand = New-Object System.Data.SqlClient.SqlCommand($TransformLoadSQL, $RuntimeConn)
            $TransformLoadCommand.CommandTimeout = 0
            $TransformLoadCommand.ExecuteNonQuery() | Out-Null

            $RuntimeConn.Close()
        
            # Find Row Count
            $RowCountCommand = New-Object System.Data.SqlClient.SqlCommand("SELECT COUNT(*) FROM [$($SchemaName)].[$($TableName)]", $DestinationConn)
            $rc = $RowCountCommand.ExecuteScalar()

            # Clean Up
            $Reader.Close() 
            $SourceConn.Close() 
            $DestinationConn.Close()

            "$($IsDependancy) [$($SchemaName)].[$($TableName)] extracted $($rc) rows to EDW Stage in $($Elapsed.Elapsed.ToString())."
            AddLogStep "$($IsDependancy) [$($SchemaName)].[$($TableName)]" $rc "Extracted to Source $($SourceCS)" $($Elapsed.ElapsedMilliseconds/1000.0)

        }
        Catch {
            "Error with table, $TableName`n$($Elapsed.Elapsed.ToString()).`n$_"
            $BulkTrans.Rollback()
        }

        Finally {
            # Clean Up
            If ($Reader.State() -eq "Open") { $Reader.Close() }
            If ($SrcConn.State() -eq "Open") { $SrcConn.Close() }
            If ($DstConn.State() -eq "Open") { $DstConn.Close() }
        }
    }
}

"Done ($($env) environment)"
exit 0; # happy

