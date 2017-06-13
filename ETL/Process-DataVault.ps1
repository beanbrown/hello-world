Param (
    [int]$Run = [int]("-1$((Get-Date).ToString("MMddHHmm"))"), # Default typically used for testing
    [string]$Environment = 'TEST', # PROD, UA, or TEST
    [string]$DestinationTable, # Only used for Stage
    [switch]$SkipMart = $false
)
$ErrorActionPreference = "SilentlyContinue"

###################################################################################################
###
### Process-DataVault.ps1
### ---------------------
###
### ETL Stage, Vaults and Marts
###
### - Winter 11/2016
###	Born
###
### - Winter 1/2017
### Updated for use with DV20 process
### Replaced LogStep logging with DV.LogMe sproc
###
### - Winter 4/2017
### Rewritten for better functions and include all steps for Raw, Business and Mart
### Added transactions to BulkCopy-SQL
###
###################################################################################################


###################################################################################################
###
### INIT
###
[void][Reflection.Assembly]::LoadWithPartialName("System.Data")
[void][Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient")

# Check
if ($DestinationTable -eq '') {
    "DestinationTable required."
    exit 1; # bonk
}

# Global Variables
$Global:Run       = $Run
$Global:Elapsed   = [System.Diagnostics.Stopwatch]::StartNew()  
$Global:Operation = "BulkCopy"
$Global:BatchTime = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff") # sql format
# $ElapsedTime  = (Get-Date).AddMilliseconds(-1*$elapsed.ElapsedMilliseconds).ToString("yyyy-MM-dd HH:mm:ss.fff")

# Local Variables
$BatchSize  = 50000
$DBNull     = [System.DBNull]::Value
$DIAG       = 1

########################################################################################################################
###
### Get-RunEnvironment 
### ------------------
###
### Return $Settings hashtable based on $Environment =~ m/(TEST|US|PROD)/
###
Function Get-RunEnvironment {
    Param (
        $Environment,
        $DstTable
    )

    If (-Not $Environment) { 
        # can we figure it out based on computer name?
        "Environment param not provided."
        If ($env:COMPUTERNAME -match "BI(EW|FS)DB(?<env>.)\d\d") {
            "Attempting to find env from servername"
            Switch ($Matches['env']) {
                P { $env = "PROD"; break }
                U { $env = "UA"; break }
                
                Default { $env = "TEST"; break; }
            }
        } Else {
            "Could not determine env from servername, using default"
            $env = "TEST"
        }
    }
    ElseIf ($Environment -match "(PROD|UA|TEST)") { 
        "Valid env passed as param"
        $env = $Environment 
    }
    Else { 
        "Invalid environment: $($Environment)"
        Exit 1
    }
    
    "Using Environment: $env"
    Switch ($env) {
        PROD { 
            $Dat = @{Etl = "ETL"; Stg = "Stage"; Biz = "Vault_Business"; Log = "ETL"; } # destination servers
            $Svr = @{Etl = "biewlip01"; Stg = "biewlip01"; Biz = "biewlip01"; Mrt = "biewlip01"; Log = "biewlip01"; } # destination servers
            $Qry = @{Query="SQLQuery";Server="SourceServer";Database="SourceDatabase"}; 
            $Mrt = @{Server="MartServer"; Database="MartDatabase"}; 
            Break; 
        }
        UA   { 
            $Dat = @{Etl = "ETL"; Stg = "Stage"; Biz = "Vault_Business"; Log = "ETL"; } # destination servers
            $Svr = @{Etl = "biewliu01"; Stg = "biewliu01"; Biz = "biewliu01"; Mrt = "biewliu01"; Log = "biewliu01"; }
            $Qry = @{Query="SQLQuery_UA";Server="SourceServer_UA";Database="SourceDatabase_UA"}; 
            $Mrt = @{Server="MartServer_UA"; Database="MartDatabase_UA"}; 
            Break;
        }
        TEST { 
            $Dat = @{Etl = "ETL"; Stg = "Stage"; Biz = "Vault_Business"; Log = "ETL"; } # destination servers
            $Svr = @{Etl = "bifsdbt01"; Stg = "bifsdbt01"; Biz = "bifsdbt01"; Mrt = "bifsdbt01"; Log = "bifsdbt01"; }
            $Qry = @{Query="SQLQuery_TEST";Server="SourceServer_TEST";Database="SourceDatabase_TEST"}; 
            $Mrt = @{Server="MartServer_TEST"; Database="MartDatabase_TEST"}; 
            Break; 
        }
    }

    # Queries
    $RawVaultQuery = "
        SELECT 
            SO = 1, 
            TB = DestinationTable, 
            SC = c.CompanyKey, 
            QY = $($Qry.Query), 
            SS = $($Qry.Server), 
            DB = $($Qry.Database),
            IC = x.IncDateColumn,
            ST = x.IncDateStamp,
            ID = x.QueryID
        FROM etl.ExtractQueries x 
            JOIN etl.etl.ExtractSources s on s.SourceID = x.SourceID 
            JOIN etl.etl.ExtractCompanies c on c.CompanyID = x.CompanyID
        WHERE DestinationTable = CASE '$($DstTable)' WHEN '' THEN DestinationTable ELSE '$($DstTable)' END 
            AND LEFT(destinationtable, 2) = 'v_'
            AND IsActive = 1
    "
    $BusinessVaultQuery = "
        SELECT DISTINCT
            SO = 1,
	        TB = b.TableName,
            SC = NULL,
            QY = N'',
            SS = '$($Mrt.Svr)',
            DB = '$($Mrt.Dat)',
            IC = NULL,
            ST = NULL,
            ID = b.BusinessLinkID
        FROM etl.etl.BusinessLinks b
		WHERE TableName = '$($DestinationTable)'
			AND _IsActive = 1
        -- FROM sys.sql_expression_dependencies d
	    --     LEFT OUTER JOIN etl.etl.ExtractQueries x ON x.DestinationTable = referenced_entity_name
	    --     LEFT OUTER JOIN etl.etl.BusinessQueries b ON b.DestinationTable = OBJECT_NAME(referencing_id)
        -- WHERE referenced_database_name IS NOT NULL
        --     AND is_ambiguous = 0
	    --     AND referenced_database_name IN ('Stage', 'Vault_3NF', 'Vault_Business', 'Vault_Operational')
	    --     AND referenced_schema_name = 'dbo'
	    --     AND x.LastLoad > ISNULL(b.LastLoad, '1-1-1900')
	    --     -- AND OBJECT_NAME(referencing_id) = CASE '$($DstTable)' WHEN '' THEN OBJECT_NAME(referencing_id) ELSE '$($DstTable)' END 
        --     AND b.IsActive = 1
    "
    $MartQuery = "
		SELECT DISTINCT 
            SO = 1,
	        TB = m.MartTableName,
            SC = NULL,
            QY = case isnull(m.$($Qry.Query), N'') 
                when N'' then N'SELECT * FROM '+STUFF(m.MartDatabase, charindex('mart_', m.MartDatabase), len('mart_'), '')+N'.'+m.MartTableName
                else m.$($Qry.Query) end,
            SS = $($Mrt.Server), 
            DB = $($Mrt.Database), 
            IC = STUFF(m.MartDatabase, charindex('mart_', m.MartDatabase), len('mart_'), ''),
            ST = NULL,
            ID = m.MartID
		FROM etl.MartQueries m
			CROSS APPLY STRING_SPLIT(m.DestinationTables, N',') dt 
			JOIN etl.ExtractQueries q ON q.DestinationTable = dt.Value
        WHERE q.DestinationTable = '$($DstTable)'
	        -- AND m._LastLoad < q.LastLoad
    "

    # $MartQuery = "
    #     DECLARE @SQL NVARCHAR(max)
    #     DECLARE @Mart NVARCHAR(128)
    # 
    #     DROP TABLE IF EXISTS #TEMP_M
    #     CREATE TABLE #TEMP_M (
	#         [SO] [INT] NOT NULL,
	#         [TB] [NVARCHAR](128) NULL,
	#         [IM] [NVARCHAR](128) NULL,
	#         [SC] [INT] NULL,
	#         [QY] [NVARCHAR](MAX) NULL,
	#         [SS] [VARCHAR](128) NOT NULL,
	#         [DB] [NVARCHAR](128) NULL,
    #         [ID] [INT] NULL
    #     )
    # 
    #     DECLARE m CURSOR FOR
	#         SELECT DISTINCT DestinationMart
	#         FROM etl.etl.MartQueries
    # 
    #     OPEN m
    #     FETCH NEXT FROM m INTO @Mart
    # 
    #     WHILE @@FETCH_STATUS = 0
    #     BEGIN
	#         SET @SQL = N'
    #             USE ['+@Mart+N'];
	# 	        INSERT INTO #TEMP_M (SO, TB, IM, SC, QY, SS, DB, ID)
	# 	        SELECT DISTINCT
	# 		        SO = b.QueryID,
	# 		        TB = b.DestinationTable,
	# 		        IM = b.DestinationMart,
	# 		        SC = x.CompanyID,
    #                 QY = b.$($Qry.Query),
    #                 SS = ''$($Svr.Mrt)'',
	# 		        DB = referenced_database_name,
    #                 ID = b.QueryID            
	# 	        FROM sys.sql_expression_dependencies d
	# 		        LEFT OUTER JOIN etl.etl.ExtractQueries x ON x.DestinationTable = referenced_entity_name
	# 		        LEFT OUTER JOIN etl.etl.MartQueries b ON b.DestinationTable = OBJECT_NAME(referencing_id)
	# 	        WHERE referenced_database_name IS NOT NULL
	# 		        AND is_ambiguous = 0
	# 		        AND referenced_database_name IN (''Stage'', ''Vault_3NF'', ''Vault_Business'', ''Vault_Operational'')
	# 		        AND referenced_schema_name = ''dbo''
	# 		        AND x.LastLoad > ISNULL(b.LastLoad, ''1-1-1900'')
	# 		        AND b.IsActive = 1'
	#         EXEC sp_executesql @SQL
    # 
	#         FETCH NEXT FROM m INTO @Mart
    #     END
    # 
    #     CLOSE m
    #     DEALLOCATE m
    # 
    #     SELECT SO, TB, IM, SC, QY, SS, DB, IC=NULL, ST=NULL, ID
    #     FROM #TEMP_M
    # 
    #     DROP TABLE IF EXISTS #TEMP_M
    # "

    Return ,@{
        "RawVaultQuery"      = $RawVaultQuery;
        "BusinessVaultQuery" = $BusinessVaultQuery;
        "MartQuery"          = $MartQuery;
        "Svr"                = $Svr;
        "Dat"                = $Dat
    }
}

###################################################################################################
###
### DV.LogMe
###   LogMe $TableName, $DestinationConn, $Elapsed, $RowCount, $Run
###
### Log to DV.Log table (typically in ETL database)
###
Function LogMe {
    Param(
        $tbl, # Table Name
        $op,  # Operation
        $sub, # Sub-Operation
        $lcn,
        $i=0, # Row Count
        $j=0, # Row Difference
        $txt, # Notes
        $ss,  # SQL Statement
        $dur  # Duration
    )

    If ($op -eq $null) { $op = $Global:Operation }
    $txt = $txt -replace "'", "''"
    $ss  = $ss  -replace "'", "''"
    $dur = [int]$dur

    # Save to ETL log
    Try {
        ($LogConn = New-Object System.Data.SQLClient.SQLConnection($Global:LogCS)).Open()
        ($LogCmd = $LogConn.CreateCommand()).CommandText = "
            exec ETL.DV.LogMe 
                @Name         = N'$($tbl)', 
                @Operation    = N'$($op)', 
                @SubOperation = N'$($sub)', 
                @LoadTime     = N'$($Global:BatchTime)', 
                @Duration     = N'$($dur)', 
                @RowsTotal    = N'$($i)', 
                @RowsDelta    = N'$($j)', 
                @Run          = N'$($Global:Run)', 
                @Notes        = N'$($txt)',
                @SQLStatement = N'$($ss)'
            "
        $LogCmd.ExecuteNonQuery() | Out-Null
    }
    Catch {
        $_.Exception.Message
        $LogCmd.CommandText
    }
    Finally {
        If ($LogConn.State -eq "Open") { $LogConn.Close() | Out-Null }
        $LogCmd.Dispose()
    }
}

########################################################################################################################
###
### Execute-SQL 
### -----------
###
### Requires an open connection, SQL Query and whether to return a scalar value or not
###  . This will not return a dataset (or datatable)
###
Function Execute-SQL {
    Param (
        $Conn,
        $SQL, 
        $ReturnScalar = $False
    )

    $essw = [System.Diagnostics.Stopwatch]::StartNew()
    Try {
        ($Command = $Conn.CreateCommand()).CommandText = $SQL
        $Command.CommandTimeout = 0
        If ($ReturnScalar) { $Result = $Command.ExecuteScalar() }
        Else               { $Result = $Null; $Command.ExecuteNonQuery() | Out-Null }
    }
    Catch {
        LogMe -tbl "{ERROR}" -op "Raw Extract" -sub "Process-DataVault.ps1/Execute-SQL" -ss "$($SQL)" -txt "$($_.Exception.Message -replace "'", "''")" -dur $essw.Elapsed.TotalMilliseconds
    }
    Finally {
        $Command.Dispose()
    }

    Return $Result
}

########################################################################################################################
###
### Get-SQLTables
### -----------
###
### Return dataset required to BulkCopy tables and perform pre/post operations in Data Vault
###  . Requires an open connection
###
Function Get-SQLTables {
    Param (
        $Conn,
        $SQL
    )

    # Find DV Objects to Process
    $gtsw = [System.Diagnostics.Stopwatch]::StartNew()
    Try {
        $Tables = New-Object System.Data.DataTable
        ($Ada = New-Object System.Data.SqlClient.SqlDataAdapter($SQL, $Conn)).Fill($Tables) | Out-Null
    }
    
    Catch {
        LogMe -tbl "{ERROR}" -op "Raw Extract" -sub "Process-DataVault.ps1/Get-SQLTables" -cs $DestinationCS -ss $SQL -txt $_.Exception.Message -dur $gtsw.Elapsed.TotalMilliseconds
        "Error extracting (SQLBulkCopy).`n$($_.Exception.Message)"
    }
    
    Return ,$Tables
}

########################################################################################################################
###
### CreateTable-FromBulkCopy
### -----------
###
###
Function CreateTable-FromBulkCopy {
    Param (
        $DstConn,
        $DstSchema,
        $DstTable,
        $SrcConn,
        $SrcSchema,
        $SrcTable,
        $SrcQuery,
        $EtlConn
    )

    $DstCommand = New-Object System.Data.SqlClient.SqlCommand("SELECT OBJECT_ID(N'[$($DstSchema)].[$($DstTable)]', N'U')", $DstConn)
    $DstCommand.CommandTimeout = 0

    If ($DstCommand.ExecuteScalar() -eq $DBNull) {
        $SrcCommand = New-Object System.Data.SqlClient.SqlCommand($SrcQuery, $SrcConn)
        $SrcCommand.CommandTimeout = 0
        [System.Data.SqlClient.SqlDataReader] $SrcReader = $SrcCommand.ExecuteReader()
        $DataTable = $SrcReader.GetSchemaTable()
        $SrcCommand.Dispose()

        $SQLTypes = @{
            'boolean'    = '[BIT]';
            'byte'       = '[NVARCHAR]';
            'char'       = '[NCHAR]';
            'datetime'   = '[DATETIME]';
            'decimal'    = '[DECIMAL](32, 16)';
            'double'     = '[FLOAT]';
            'guid'       = '[UNIQUEIDENTIFIER]'
            'int16'      = '[SMALLINT]';
            'int32'      = '[INT]';
            'int64'      = '[BIGINT]';
            'money'      = '[MONEY]';
            'sbyte'      = '[NVARCHAR]';
            'single'     = '[REAL]';
            'string'     = '[NVARCHAR]';
            'timespan'   = '[TIME]';
            'timestamp'  = '[BIGINT]';
            'tinyint'    = '[INT]';
        }
        $DateColumns = ','
        $DataTable.Rows | ? { $_.ColumnName -match "_DT$" } | % {
            $DateColumns += "$($_.ColumnName.Replace("_DT", "_DD")) as convert(DATE, $($_.ColumnName)),"
            $DateColumns += "$($_.ColumnName.Replace("_DT", "_DK")) as convert(INT, convert(varchar(8), $($_.ColumnName), 112)),"
            $DateColumns += "$($_.ColumnName.Replace("_DT", "_TT")) as convert(TIME, $($_.ColumnName)),"
            $DateColumns += "$($_.ColumnName.Replace("_DT", "_TK")) as convert(INT, replace(convert(varchar(8), $($_.ColumnName), 108), ':', '')),"
        }

        # $(@('NOT NULL', 'NULL')[$_.AllowDBNull]) -- all cols in Stage NULLABLE, 2.2.3
        $CreateTableSQL = "
            CREATE TABLE [$($DstSchema)].[$($DstTable)] (
                $((($DataTable.Rows | % { 
                    $DataType = $_.DataType.Name
                    If ($_.DataTypeName -match "(timestamp|money|tinyint)") { $DataType = $_.DataTypeName}
                    "[$($_.ColumnName)] $($SQLTypes."$($DataType)")$(@('', "($($_.ColumnSize))")[($_.NumericPrecision -eq 255) -and -not ($_.DataTypeName -match '(date|bit|tinyint|uniqueidentifier|timestamp)')]) NULL" 
                }) -Join ",`n") -replace "\(\)", '')
                $DateColumns
            )"
        $CreateTableCommand = New-Object System.Data.SqlClient.SqlCommand($CreateTableSQL, $DstConn)
        $CreateTableCommand.ExecuteNonQuery() | Out-Null

        If ($SrcReader.State -eq "Open") { $SrcReader.Close() | Out-Null; $SrcReader.Dispose() | Out-Null; } 
    }
}


#################################################################################################################################
###
### MAIN
### ----
### 
### Sections to Process: (3.3)
###  . Stage (Stage)
###  . Hub (Raw)
###  . Link (Business)
###  . Satellite (Raw)
###  . Dimension (Mart)
###  . Fact (Mart)
### 
### Each Section Follows this Process:
###  . Find Tables to Process (if any)
###  . Pre-Execution Steps
###  . BulkCopy the tables
###  . Post-Execution Steps (typically, at least updatin _LastLoad)
###
### Dependancies
###  . ETL tables: RawQueries, BusinessQueries, MartQueries
###  . LOG table: ETL.DV.Log
###
### Conventions
###  . v_First_Second represents a vault table/Must identify *_BK cols for unique primary key
###  .. h_First hub will be created in RAW
###  .. s_First_Second hub will be created in RAW
###  .. And hubs identified as "First" must have same BK's
###
###  . Vault_Business represents the Business Vault
###  .. For testing, if views are not in BusinessQueries table, they must be in the Vault_Business database in the BIZ schema
###
###  . Mart_Name represents a Mart database
###  .. For testing, if views are not in MartQueries table, they must be in the Mart_Name database in the MRT schema
###
#################################################################################################################################

# TEST, UA, or PROD
if ($DIAG) { ":Get Settings for Environment" }
$Settings = Get-RunEnvironment $Environment $DestinationTable

# Connections
if ($DIAG) { ":Setup Connections" }
($StgConn = New-Object System.Data.SqlClient.SqlConnection("Server=$($Settings.Svr.Stg);Database=$($Settings.Dat.Stg);Trusted_Connection=True;MultipleActiveResultSets=true;")).Open()
($EtlConn = New-Object System.Data.SqlClient.SqlConnection("Server=$($Settings.Svr.Etl);Database=$($Settings.Dat.Etl);Trusted_Connection=True;MultipleActiveResultSets=true;")).Open()
($BizConn = New-Object System.Data.SqlClient.SqlConnection("Server=$($Settings.Svr.Biz);Database=$($Settings.Dat.Biz);Trusted_Connection=True;MultipleActiveResultSets=true;")).Open()
$Global:LogCS = "Server=$($Settings.Svr.Log);Database=$($Settings.Dat.Log);Trusted_Connection=True;"

########################################################################################################################
###
### BulkCopy
### -----------
###
### BulkCopy SQL Table from Source to Destination Server.Database
###  . Requires open Connections to Source, Destination and ETL Databases
###  . Source Connection will be Created Based on Information from the $Tables HashTable
###
### Assumptions/Considerations
###  . SQLBulkCopy does not handle BLOBs, which may cause issues; consider performing that operation manually
###  . If the table does not exist at the destination, it is created; consider using an sproc to create the table
###  . The initial truncate and BulkCopy operations happen inside a transaction, which can be rolled back if needed
###
$sbBulkCopy = {
    Param (
        $SrcConn, 
        $SrcSchema,
        $SrcTable,
        $SrcQuery,
        $DstConn, 
        $DstSchema,
        $DstTable
    )

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $RetVal = @{err=0; tbl=$DstTable; op="SqlBulkCopy"; sub="ExecuteReader"; ss=''; dur=0; i=0; txt=''; }
    $BatchSize  = 50000

    Try {
        # Get the source table
        $SrcCommand = New-Object System.Data.SqlClient.SqlCommand($SrcQuery, $SrcConn)
        $SrcCommand.CommandTimeout = 0
        [System.Data.SqlClient.SqlDataReader] $Reader = $SrcCommand.ExecuteReader()
        $DataTable = $Reader.GetSchemaTable()
        $SrcCommand.Dispose()

        # Bulk insert into destincation table
        $RetVal.sub = "Truncate and BulkCopy"
        $BulkTrans = $DstConn.BeginTransaction()
        $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DstConn, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock, $BulkTrans)
        $BulkCopy.DestinationTableName = "[$($DstSchema)].[$($DstTable)]"
        $BulkCopy.BulkCopyTimeout = 0
        $BulkCopy.BatchSize = $BatchSize
        $BulkCopy.EnableStreaming = $True
        $DataTable.Rows | % { [Void]$BulkCopy.ColumnMappings.Add($_.ColumnName, $_.ColumnName) }

        $TruncateCommand = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE [$($DstSchema)].[$($DstTable)]", $DstConn, $BulkTrans)
        $TruncateCommand.ExecuteNonQuery() | Out-Null

        $RetVal.sub = "WriteToServer"
        $BulkCopy.WriteToServer($Reader)
        $BulkTrans.Commit()
        $BulkCopy.Close()
        $BulkCopy.Dispose()
        $DataTable.Clear()
        $DataTable.Dispose()

        # Find Row Count
        $RetVal.sub = "Get RowCount"
        $RowCountCommand = New-Object System.Data.SqlClient.SqlCommand("SELECT COUNT(*) FROM [$($DstSchema)].[$($DstTable)]", $DstConn)
        $RetVal.i = $RowCountCommand.ExecuteScalar()        
        $RowCountCommand.Dispose()
    }

    Catch {
        If ($BulkTrans -ne $null) { $BulkTrans.Rollback() }
        $RetVal.tbl = "{ERROR}"
        $RetVal.sub = $Table
        $RetVal.err = 1
        $RetVal.txt = $_.Exception.Message
    }

    Finally {
        # Clean Up
        If ($Reader.State -eq "Open") { $Reader.Close() | Out-Null; $Reader.Dispose() | Out-Null; }
    }

    $RetVal.dur = [int]$sw.Elapsed.TotalMilliseconds
    Return $RetVal
}

# ScriptBlock - ExecuteSQL
$sbExecuteSQL = {
    Param ($SQL, $CS)
    [void][Reflection.Assembly]::LoadWithPartialName("System.Data")
    [void][Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient")
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $RetVal = @{err=0; tbl=''; op="SqlBulkCopy"; sub="ExecuteSQL ScriptBlock"; ss=$SQL; dur=0; i=0; txt=$CS; }
    Try {
        ($Conn = New-Object System.Data.SqlClient.SqlConnection($CS)).Open()
        ($Command = $Conn.CreateCommand()).CommandText = $SQL
        $Command.CommandTimeout = 0
        $Command.ExecuteNonQuery() | Out-Null
    } 
    Catch {
        $RetVal.tbl = "{ERROR}"
        $RetVal.err = 1
        $RetVal.txt = $_.Exception.Message
    }
    Finally {
        $Command.Dispose()
    }

    $RetVal.dur = [int]$sw.Elapsed.TotalMilliseconds
    Return $RetVal
}

# Init runspaces
$Pool = [RunSpaceFactory]::CreateRunspacePool(1, [int]$env:NUMBER_OF_PROCESSORS+1)
$Pool.ApartmentState = "MTA"
$Pool.Open()
$rsBiz = @()
$rsMrt = @()

# Stage - Disable indexes for bulk copy
Execute-SQL $StgConn "IF OBJECT_ID(N'Stage.dbo.[$($DestinationTable)]', N'U') IS NOT NULL ALTER INDEX ALL ON Stage.dbo.[$($DestinationTable)] DISABLE;"

# Stage - Incremental Loading
if ($DIAG) { ":Stage" }
$RawTables = Get-SQLTables $EtlConn $Settings.RawVaultQuery
$RawTables | % { 
    # Source may vary; build and close
    ($sc = New-Object System.Data.SqlClient.SqlConnection("Server=$($_.SS);Database=$($_.DB);Trusted_Connection=true;Connection Timeout=600;MultipleActiveResultSets=true;")).Open()
    $ss = 'dv'
    $st = $_.TB
    $sq = @($_.QY, "SELECT * FROM DV.[$($st)] <<IncrementalPredicate>>")[$_.QY -eq $DBNull] # if null, use view in DV schema
    $sq = $sq -replace "<<IncrementalPredicate>>", ("WHERE $($_.IC) >= <<IncrementalDateStamp>>", "")[($_.IC -eq $DBNull) -or ($_.ST -eq $DBNull)] # ignore if either Incremental Col is NULL
    $sq = $sq -replace "<<IncrementalDateStamp>>", ("'$($_.ST)'", "'1-1-1900'")[$_.ST -eq $DBNull] # beginning of time if IncDateStamp is NULL, allows for this to be put in QY; handles first time
    $dc = $StgConn
    $ds = 'dbo'
    $dt = $_.TB
    $ec = $EtlConn

    # Create destination if it doesn't exist
    CreateTable-FromBulkCopy $dc $ds $dt $sc $ss $st $sq $ec

    # BulkCopy into destination
    $rv = icm -ScriptBlock $sbBulkCopy -ArgumentList $sc, $ss, $st, $sq, $dc, $ds, $dt
    LogMe -tbl $rv.tbl -op $rv.op -sub $rv.sub -txt $rv.txt -i $rv.i -dur $rv.dur

    # Create links, if any
    Execute-SQL $EtlConn "EXEC dv.LoadStage @TABLE=N'$($st)', @Run=N'$Run'"

    if ($sc.State -eq "Open") { $sc.Close() }
}

# Stage - rebuild indexes on imported table
Execute-SQL $StgConn "IF OBJECT_ID(N'Stage.dbo.[$($DestinationTable)]', N'U') IS NOT NULL ALTER INDEX ALL ON Stage.dbo.[$($DestinationTable)] REBUILD;"

# Raw (aka Business Vault) 12.1
if ($DIAG) { ":Raw Vault Begin" }
$RawTables | % {
    $TableName = $_.TB
    $CompanyKey = $_.SC
    $SourceLocation = "[$($_.SS)].[$($_.DB)]"
    if ($DIAG) { ":$($TableName) Start" }

    $HubName = $SatName = ''
    If ($TableName -match "^v_(?<ht>.+)_(?<st>.+)$") {
        $HubName = "h_$($Matches.ht)"
        $SatName = "s_$($Matches.ht)_$($Matches.st)"
    }
    
    # Load Hub
    Execute-SQL $EtlConn "EXEC dv.LoadHub @TABLE=N'$TableName', @HUB=N'$HubName', @CompanyKey=N'$CompanyKey', @SourceLoc=N'$SourceLocation', @Run=N'$Run'" 
    
    # Load Sat
    $Pipeline = [System.Management.Automation.PowerShell]::Create()
    $Pipeline.AddScript($sbExecuteSQL) | Out-Null
    $Pipeline.AddArgument("EXEC dv.LoadSat @TABLE=N'$TableName', @SAT=N'$SatName', @CompanyKey=N'$CompanyKey', @SourceLoc=N'$SourceLocation', @Run=N'$Run'") | Out-Null
    $Pipeline.AddArgument("Server=$($Settings.Svr.Etl);Database=$($Settings.Dat.Etl);Trusted_Connection=True;MultipleActiveResultSets=true;") | Out-Null
    $Pipeline.RunspacePool = $Pool

    $rsBiz += [PSCustomObject]@{ Pipe = $Pipeline; Status = $Pipeline.BeginInvoke() }    

    if ($DIAG) { ":$($TableName) End" }
}

# Business
if ($DIAG) { ":Biz Vault Begin" }
$BusinessTables = Get-SQLTables $BizConn $Settings.BusinessVaultQuery
$BusinessTables | % {
    $TableName = $_.TB
    if ($DIAG) { ":$($TableName) Start" }

    # Load Link
    $Pipeline = [System.Management.Automation.PowerShell]::Create()
    $Pipeline.AddScript($sbExecuteSQL) | Out-Null
    $Pipeline.AddArgument("EXEC dv.LoadBiz @TABLE=N'$TableName', @Run=N'$Run'") | Out-Null
    $Pipeline.AddArgument("Server=$($Settings.Svr.Etl);Database=$($Settings.Dat.Etl);Trusted_Connection=True;MultipleActiveResultSets=true;") | Out-Null
    $Pipeline.RunspacePool = $Pool

    $rsBiz += [PSCustomObject]@{ Pipe = $Pipeline; Status = $Pipeline.BeginInvoke() }    
        
    if ($DIAG) { ":$($TableName) End" }
}
While ($rsBiz.Status.IsCompleted -NotContains $True) { }
$rsBiz | % {
    # EndInvoke method retrieves the results of the asynchronous call
    $Results = $_.Pipe.EndInvoke($_.Status)
    $_.Pipe.Dispose()
    LogMe -tbl $Results.tbl -op $Results.op -sub $Results.sub -ss $Results.ss -txt $Results.txt -i $Results.i -dur $Results.dur
}

if ($DIAG) { ":Raw Complete" }
if ($DIAG) { ":Biz Complete" }

# Marts
if (-not $SkipMart) {
    if ($DIAG) { ":Mart Begin" }
    $MartTables = Get-SQLTables $EtlConn $Settings.MartQuery
    if (@($MartTables).Count -ne 0) { 
        $MartTables | % {
            $sc = $BizConn
            $ss = $_.IC
            $st = $_.TB
            $sq = @($_.QY, "SELECT * FROM [$($ss)].[$($st)] <<IncrementalPredicate>>")[$_.QY -eq $DBNull] # if null, use view in DV schema
            $sq = $sq -replace "<<IncrementalPredicate>>", ("WHERE $($_.IC) >= <<IncrementalDateStamp>>", "")[($_.IC -eq $DBNull) -or ($_.ST -eq $DBNull)] # ignore if either Incremental Col is NULL
            $sq = $sq -replace "<<IncrementalDateStamp>>", ("'$($_.ST)'", "'1-1-1900'")[$_.ST -eq $DBNull] # beginning of time if IncDateStamp is NULL, allows for this to be put in QY; handles first time
            ($dc = New-Object System.Data.SqlClient.SqlConnection("Server=$($_.SS);Database=$($_.DB);Trusted_Connection=true;Connection Timeout=600;MultipleActiveResultSets=true;")).Open()
            $ds = 'dbo'
            $dt = $_.TB
            $ec = $EtlConn

            # Create destination if it doesn't exist
            CreateTable-FromBulkCopy $dc $ds $dt $sc $ss $st $sq $ec

            # Load Mart
            $Martline = [System.Management.Automation.PowerShell]::Create()
            $Martline.AddScript($sbBulkCopy) | Out-Null
            $Martline.AddArgument($sc) | Out-Null
            $Martline.AddArgument($ss) | Out-Null
            $Martline.AddArgument($st) | Out-Null
            $Martline.AddArgument($sq) | Out-Null
            $Martline.AddArgument($dc) | Out-Null
            $Martline.AddArgument($ds) | Out-Null
            $Martline.AddArgument($dt) | Out-Null
            $Martline.RunspacePool = $Pool

            $rsMrt += [PSCustomObject]@{ Pipe = $Martline; Status = $Martline.BeginInvoke() }    
        }    
        While ($rsMrt.Status.IsCompleted -NotContains $True) {}
        $rsMrt | % {
            # EndInvoke method retrieves the results of the asynchronous call
            $Results = $_.Pipe.EndInvoke($_.Status)
            $_.Pipe.Dispose()
            LogMe -tbl $Results.tbl -op $Results.op -sub $Results.sub -ss $Results.ss -txt $Results.txt -i $Results.i -dur $Results.dur
        }
    }
    
    if ($DIAG) { ":Mart Complete" }
}

# Clean up
# if ($DIAG) { ":Truncate Stage" } # 12.3 !! Implement this once incremental loads are implemented
# Execute-SQL $EtlConn "TRUNCATE TABLE Stage.dbo.[$($TableName)]"

if ($DIAG) { ":Clean Up" }
If ($StgConn.State -eq "Open") { $StgConn.Close() | Out-Null }
If ($EtlConn.State -eq "Open") { $EtlConn.Close() | Out-Null }
If ($BizConn.State -eq "Open") { $BizConn.Close() | Out-Null }

$Pool.Close()
$Pool.Dispose()

if ($DIAG) { ":Fin (Run $($Run)), completed in {0:n2}s" -f $($Global:Elapsed.Elapsed.TotalSeconds) }
