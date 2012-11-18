<?php

$dbhost = 'localhost';
$dbname = 'fn';
$dbuser = 'root';
$dbpass = '';

$db = get_db($dbhost, $dbuser, $dbpass, $dbname);
$symbols = get_symbols($db);

foreach($symbols as $symbol) {
    $most_recent_date = strtotime('January 1, 1950');
    $yesterday = strtotime("yesterday");
    
    $history = get_symbol_history($symbol, $db, 1);
    if (!empty($history)) {
        $most_recent_date = strtotime($history[0]['dt'] . ' + 1 day');
    }
    
    if ($most_recent_date < $yesterday) {
        $data = get_symbol_data($symbol, $most_recent_date);
        update_history($symbol, $data, $most_recent_date, $db);
    }
}

function update_history($symbol, $data, $most_recent_date, $db)
{
    $query_data = array();
    
    foreach($data as $line) {
        $line_data = str_getcsv($line);
        if (strtotime($line_data[0]) > $most_recent_date) {
            $query_data[] = "('$symbol', '{$line_data[0]}', {$line_data[1]}, {$line_data[2]}, {$line_data[3]}, {$line_data[4]}, {$line_data[5]}, {$line_data[6]})";
        }
        if (sizeof($query_data) >= 100) {
            update_history_batch($query_data, $db);
            $query_data = array();
        }
    }
    
    if (!empty($query_data)) {
        update_history_batch($query_data, $db);
    }
}

function update_history_batch($query_data, $db)
{
    $query = "INSERT INTO history (symbol, dt, open, high, low, close, volume, adjusted_close) VALUES " . implode(',', $query_data);
    mysql_query($query, $db);
    if (mysql_errno($db)) {
        echo "MySQL error ".mysql_errno($db).": ".mysql_error($db)."\nWhen executing:\n$query\n";
    }
}

function get_symbol_data($symbol, $from_dt)
{
    // Get the from date
    $dt = getdate($from_dt);
    // Get today's date
    $tdt = getdate();
    $symbol = urlencode($symbol);
    // Yahoo stores month as zero based
    $dt['mon'] -= 1;
    $tdt['mon'] -= 1;
    
    //$url = "http://ichart.finance.yahoo.com/table.csv?s={$symbol}&a={$dt['mon']}&b={$dt['mday']}&c={$dt['year']}&ignore=.csv";
    $url = "http://ichart.finance.yahoo.com/table.csv?s={$symbol}&a={$dt['mon']}&b={$dt['mday']}&c={$dt['year']}&d={$tdt['mon']}&e={$tdt['mday']}&f={$tdt['year']}&g=d&ignore=.csv";
    $data  = file_get_contents($url);
    
    $lines = str_getcsv($data, "\n");
    
    return $lines;
}

function get_symbol_history($symbol, $db, $limit = false)
{
    $history = array();
    
    $query = "
    SELECT
        *
    FROM
        history
    WHERE
        symbol = '$symbol'
    ORDER BY
        dt desc
    ";
    
    if ($limit) {
        $query .= " LIMIT $limit";
    }
    
    $res = mysql_query($query, $db);
    if (!$res) {
        die('Invalid query: ' . mysql_error());
    }
    
    while($row = mysql_fetch_array($res)) {
        $history[] = $row;
    }
    
    return $history;
}

function get_symbols($db)
{
    $symbols = array();
    
    $query = "
    SELECT
        symbol
    FROM
        symbols
    ";
    
    $res = mysql_query($query, $db);
    if (!$res) {
        die('Invalid query: ' . mysql_error());
    }
    
    while($row = mysql_fetch_array($res)) {
        $symbols[] = $row['symbol'];
    }
    
    return $symbols;
}

function get_db($dbhost, $dbuser, $dbpass, $dbname)
{
    $db = mysql_connect($dbhost, $dbuser, $dbpass);
    if (!$db) {
        die('sorry - cannot connect to db');
    }
    
    mysql_selectdb($dbname, $db);
    
    return $db;
}



