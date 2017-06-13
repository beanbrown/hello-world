Param (
    $cron = "12 0 12 25 *",
    $datestamp = (Get-Date),
    $run = -1,
    $environment = "test"
)
if (($cron.split()).count -ne 5) { return -1 }

########################################################################################################################
## Execute-SQL
## -----------
## Updated so only sets IsReady=1
## - must be turned off somewhere else.
##

[void][Reflection.Assembly]::LoadWithPartialName("System.Data")
[void][Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient")

$ErrorActionPreference = "SilentlyContinue"
if ($Run -eq $nul) { $Run = -1 }

$DBNull           = [System.DBNull]::Value
$Global:ii        = 0
$Global:Run       = $Run
$Global:Elapsed   = [System.Diagnostics.Stopwatch]::StartNew()
$Global:StartRun  = [System.Diagnostics.Stopwatch]::StartNew()  
$Global:Operation = "Parse CRONTAB"
$Global:BatchTime = (get-date).ToString("yyyy-MM-dd HH:mm:ss.fff") # sql format

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
    PROD { $sqlsrvr = "biewlip01"; $database = "ETL"; break; }
    UA   { $sqlsrvr = "biewliu01"; $database = "ETL"; break; }
    TEST { $sqlsrvr = "bifsdbt01"; $database = "ETL"; break; }
}
$cs = "server=$sqlsrvr;database=$database;trusted_connection=true;"

########################################################################################################################
## Execute-SQL
## -----------
##
Function Execute-SQL {
    Param (
        $ConnectionString,
        $SQL, 
        $ReturnScalar = $False
    )

    Try {
        ($Conn = New-Object System.Data.SQLClient.SQLConnection($ConnectionString)).Open()
        ($Command = $Conn.CreateCommand()).CommandText = $SQL
        $Command.CommandTimeout = 0
        If ($ReturnScalar) { $Result = $Command.ExecuteScalar() }
        Else               { $Command.ExecuteNonQuery() | Out-Null }
    }
    Catch {
        LogMe -tbl "{ERROR}" -op "Raw Extract" -sub "Extract-VIPData.ps1/Execute-SQL" -cs $Global:LogCS -ss "$($SQL)" -txt "$($_.Exception.Message -replace "'", "''")"
    }
    Finally {
        If ($Conn.State -eq "Open") { 
            $Conn.Close() | Out-Null
        }
        $Command.Dispose()
    }

    Return $Result
}

########################################################################################################################
## LogMe
## -----------
##
Function LogMe {
    Param(
        $tbl, # Table Name
        $op,  # Operation
        $sub, # Sub-Operation
        $cs,  # Connection String
        $i=0, # Row Count
        $j=0, # Row Difference
        $txt, # Notes
        $dur, # Notes
        $ss   # SQL Statement
    )

    if ($op -eq "") { $op = $Global:Operation }
    if ($dur -eq "") { $dur = $Global:Elapsed.ElapsedMilliseconds.ToString() }

    # Save to ETL log
    Try {
        ($logsql = New-Object System.Data.SQLClient.SQLConnection($cs)).Open()
        ($logcmd = $logsql.CreateCommand()).CommandText = "exec ETL.DV.LogMe 
                                    @Name         = N'$($tbl)', 
                                    @Operation    = '$($op)', 
                                    @SubOperation = '$($sub)', 
                                    @LoadTime     = '$($Global:BatchTime)', 
                                    @Duration     = '$($dur)', 
                                    @RowsTotal    = $($i), 
                                    @RowsDelta    = $($j), 
                                    @Run          = '$($Global:Run)', 
                                    @Notes        = N'$($txt -replace "'", "''")',
                                    @SQLStatement = N'$($ss -replace "'", "''")'"
        $logcmd.CommandTimeout = 0
        $logcmd.ExecuteNonQuery() | Out-Null
    }
    Catch {
        $_.Exception.Message
        $logcmd.CommandText
    }
    Finally {
        if ($logsql.State -eq "Open") {
            $logsql.Close | Out-Null
        }
        $logcmd.Dispose()
    }
}

########################################################################################################################
## Parse-Crontab
## -----------
##
Function Parse-Crontab {
Param (
    $c, # crontab
    $d  # get-date
)

    if ($d -eq $nul) { $curr = Get-Date }
    else { $curr = $d }
    $cronArray = $c.Split()
    if ($cronArray.Count -ne 5) { return -1 }

    # Minute
    if ($cronArray[0] -eq "*") { $min = 1 }
    elseif (($cronArray[0] -match "^\d+$") -and ($cronArray[0] -eq $curr.Minute)) { $min = 1 }
    elseif ($cronArray[0] -match "^(?<fr>\d+)-(?<to>\d+)$") {
    if (($curr.Minute -ge $matches.fr) -and ($curr.Minute -le $matches.to)) { $min = 1 }
    else { $min = 0 }
    }
    elseif ($cronArray[0] -match "^\d+(,\d+)*$") {
        $min = 0
        $cronArray[0].split(",") | % { if ($_ -eq $curr.Minute) { $min = 1 } }
    }
    else { $min = 0 }

    # Hour 
    if ($cronArray[1] -eq "*") { $hour = 1 }
    elseif (($cronArray[1] -match "^\d+$") -and ($cronArray[1] -eq $curr.Hour)) { $hour = 1 }
    elseif ($cronArray[1] -match "^(?<fr>\d+)-(?<to>\d+)$") {
    if (($curr.Hour -ge $matches.fr) -and ($curr.Hour -le $matches.to)) { $hour = 1 }
    else { $hour = 0 }
    }
    elseif ($cronArray[1] -match "^\d+(,\d+)*$") {
        $hour = 0
        $cronArray[1].split(",") | % { if ($_ -eq $curr.Hour) { $hour = 1 } }
    }
    else { $hour = 0 }

    # Day of Month
    if ($cronArray[2] -eq "*") { $day = 1 }
    elseif (($cronArray[2] -match "^\d+$") -and ($cronArray[2] -eq $curr.Day)) { $day = 1 }
    elseif ($cronArray[2] -match "^(?<fr>\d+)-(?<to>\d+)$") {
    if (($curr.Day -ge $matches.fr) -and ($curr.Day -le $matches.to)) { $day = 1 }
    else { $day = 0 }
    }
    elseif ($cronArray[2] -match "^\d+(,\d+)*$") {
        $day = 0
        $cronArray[2].split(",") | % { if ($_ -eq $curr.Day) { $day = 1 } }
    }
    else { $day = 0 }

    # Month
    $c3 = $cronArray[3]
    $c3 = $c3.Replace("Jan", 1); $c3 = $c3.Replace("Feb", 2); $c3 = $c3.Replace("Mar", 3)
    $c3 = $c3.Replace("Apr", 4); $c3 = $c3.Replace("May", 5); $c3 = $c3.Replace("Jun", 6)
    $c3 = $c3.Replace("Jul", 7); $c3 = $c3.Replace("Aug", 8); $c3 = $c3.Replace("Sep", 9)
    $c3 = $c3.Replace("Oct", 10); $c3 = $c3.Replace("Nov", 11); $c3 = $c3.Replace("Dec", 12)
    if ($c3 -eq "*") { $month = 1 }
    elseif (($c3 -match "^\d+$") -and ($c3 -eq $curr.Month)) { $month = 1 }
    elseif ($c3 -match "^(?<fr>\d+)-(?<to>\d+)$") {
    if (($curr.Month -ge $matches.fr) -and ($curr.Month -le $matches.to)) { $month = 1 }
    else { $month = 0 }
    }
    elseif ($c3 -match "^\d+(,\d+)*$") {
        $month = 0
        $c3.split(",") | % { if ($_ -eq $curr.Month) { $month = 1 } }
    }
    else { $month = 0 }

    # Day of Week
    $c4 = $cronArray[4]
    $c4 = $c4.Replace("Sun", 0)
    $c4 = $c4.Replace("Mon", 1)
    $c4 = $c4.Replace("Tue", 2)
    $c4 = $c4.Replace("Wed", 3)
    $c4 = $c4.Replace("Thu", 4)
    $c4 = $c4.Replace("Fri", 5)
    $c4 = $c4.Replace("Sat", 6)
    if ($c4 -eq "*") { $dow = 1}
    elseif (($c4 -match "^\d+$") -and ($c4 -eq $curr.DayOfWeek.value__)) { $dow = 1 }
    elseif ($c4 -match "^(?<fr>\d+)-(?<to>\d+)$") {
    if (($curr.DayOfWeek.value__ -ge $matches.fr) -and ($curr.DayOfWeek.value__ -le $matches.to)) { $dow = 1 }
    else { $dow = 0 }
    }
    elseif ($c4 -match "^\d+(,\d+)*$") {
        $dow = 0
        $c4.split(",") | % { if ($_ -eq $curr.DayOfWeek.value__) { $dow = 1 } }
    }
    else { $dow = 0 }

    if (
        ($min -eq 1) -And
        ($hour -eq 1) -And
        ($day -eq 1) -And
        ($month -eq 1) -And
        ($dow -eq 1)
    ) { 1 }
    else { 0 }
}

($Conn = New-Object System.Data.SQLClient.SQLConnection($cs)).Open()
($Command = $Conn.CreateCommand()).CommandText = "select queryid, crontab from etl.ExtractQueries where crontab is not null"
$Command.CommandTimeout = 0
$Reader = $Command.ExecuteReader()
$Table = New-Object “System.Data.DataTable”
$Table.Load($Reader)
$Conn.Close()

$Table | % { 
    $res = Parse-Crontab -c $_.crontab -d $DateStamp
    if ($res -eq 1) { 
        # "update etl.ExtractQueries set IsReady = $($res) where QueryID = $($_.QueryID)"
        Execute-SQL -ConnectionString $cs -SQL "update etl.ExtractQueries set IsReady = $($res) where QueryID = $($_.QueryID)"
    }
    # "update etl.ExtractQueries set IsReady = $($res) where QueryID = $($_.QueryID)"
}

########################################################################################################################
########################################################################################################################
## # Find last valid time
## $gd = Get-Date
## $res = $i = 0
## while (($res -eq 0) -and ($i -ge -130000)) { # 50k mins is a month and change
## $res = parse-crontab -c $cron -d $gd.AddMinutes($i--)
## } $i++
## 
## $gd.AddMinutes($i) # Last valid time
## $i # How many mins ago?

