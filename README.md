# COMET aka COMplex EvenT processor

NB! This tool is a product of a thesis project!  

Comet is for processing textual data line-by-line as a stream of events and providing Esper EPL query functionality on that data. Mostly oriented for processing streaming logs. Sample use-cases:  
* filter out certain log entries (and do alerting?)
* create statistical output (throughput for example)
* aggregate different logs
* join complex multi-line logs into decent log entries
* enrich logs with external data
* ....
* or all of them together  

Some use-case scenarios from esper documentation:  
* Business process management and automation (process monitoring, BAM, reporting exceptions, operational intelligence)
* Finance (algorithmic trading, fraud detection, risk management)
* Network and application monitoring (intrusion detection, SLA monitoring)
* Sensor network applications (RFID reading, scheduling and control of fabrication lines, air traffic)

## Contents of readme

**[Overview](#overview)** - short overview of data flow and main internal components in Comet  
**[Roadmap](#roadmap)** - thoughts about future improvements  
**[Building and usage](#building-and-usage)** - how to build and use Comet  
**[Logging](#logging)** - how to enable & configure logging  
**[Example usage](#example-command-line-interactive-usage)** - example uses of Comet from command line    
**[Daemonize and manage](#running-comet-daemonized)** - how to run Comet as background process  
**[Secrets](#secrets)** - how to use secrets for external resources  
**[Conf file](#monitor-configuration-file)** - configuration file manual  
* **[Input definitions](#input-definitions)**  
* **[Pattern definitions](#pattern-definitions)**  
* **[Query definitions](#query-definitions)**  
* **[Output definitions](#output-definitions)**  

**[Esper query specifics](#esper-query-specifics)** - some specific help on esper queries  

## Overview

### Data flow

```
             1. stdin,files              3. Esper EPL
                     |                          |
    Program flow: data input -> parse data -> query -> result output
                                   |                        |
                     2. grok,regex,json,csv        4. stdout,file
```

##### 1. INPUT - read input from various sources, line by line  
Read stdin, files or both of them together. Multiple sources of same input type can be used (for example multiple files). Plain text and gzipped files both are handeled.  

##### 2. PARSE - parse every line into structured object  
For parsing, regex named groups is used internally. For better usability, Grok expressions can be used (grok is compiled to regex runtime). In addition to regex, some complex data types are defined internally that regex can't parse efficiently. Currently only supported special types are JSON and URLARGS.  

##### 3. QUERY - select, filter, aggregate, join, split and do all sorts of magic with data  
This is a complicated one and can hurt your brain in the end. It's best to start with:  
[Esper indroduction](http://www.espertech.com/esper/)  
.. then study  
[Esper solution patterns](http://www.espertech.com/esper/solution-patterns/)  
... and then move to   
[Esper singl-page html referece documentation](http://esper.espertech.com/release-8.4.0/reference-esper/html_single/index.html)  

##### 4. OUTPUT - output results to one of supported output types  

Various output types can be used together. Different Esper EPL query results can be directed to different type of outputs (or silenced if needed). For example one query outputs to file, one samples metrics to zabbix and two more queries alert to slack and email.

### Technologies and architecture

High level overview of technologies and vocabulary used.  

```
 
   OS -------------+
   | COMET* / JVM ---+          # multiple Comet instances can run in a host
   | | MONITOR* -------+        # multiple monitor-s can run in an Comet instance
   |_|  | ESPER ---------+      # there is ome esper runtime per monitor
     |__|  | MODULE* ------+    # multiple modules (esper deployments) can run in esper runtime (for example a file that has multiple statements in it)
        |__|  | STATEMENT* --+  # multiple statements (esper query - like SQL) can be combined as a module (esper deployment)
           |__|  |           |
              |__|           |
                 |___________|
```

**Comet** - *java* - it's the main workhorse of the package (read inputs, parse data, execute queries and output the results). Multiple Comet instances can run in same host.   
**Monitor** - this is a configuration collection of Comet that can be run within working instance Comet. Multiple monitors can be loaded and run within one Comet instance.

## Building and usage

```bash
## build and use locally
gradle clean shadowJar               # build 
./comet                                 # run Comet

## build final artifact and use it
gradle clean distTar                 # build 
                                        # unpack and navigate to target dir
find build/distributions/ -name "*.tgz" -exec tar -C build/distributions/ -xf {} \; && cd $(find build/distributions/ -type d -name "comet-*")
./bin/comet                             # run Comet
```

## Logging

Log configuration (log4j2) is generated during startup in code. Logging can be enabled via command line parameters and configuration file. First log configuration will be used. This means if logging is enabled via command line params, log configuration in configuration file will be disregarded. And as `comet` can be run using multiple configuration files, log configuration of first one will be used.


## Example command line interactive usage

All example commands are for use in local development mode (executable path is `./comet`). To use these examples on pre-built distribution, replace exec command with `./bin/comet`.

##### 1.  Simplest of things  

```bash
echo "foobar" | ./comet
```

```json
{"data":"foobar","eventType":"events"}
```

##### 2. Event types and Esper EPL
All lines at least match event type `events`. Event types can be queried via Esper EPL (Esper processing language) - statements.  
```bash
echo "foobar" | ./comet "select data from events"
```

```json
{"data":"foobar"}
```

##### 3. Other event types and output formatting
A special prefixed log format is parsed into embedded event type `logevents`.  
Output can be formatted as needed
```bash
echo $(date -Iseconds)$'\tlocalhost\t1234\tdata' | ./comet  --out-format 'Logline from ${host} @ ${logts}. Source PID was ${pid}.' "select * from logevents where host = 'localhost'"
```

```
Logline from localhost @ 2020-04-03T12:12:33+03:00. Source PID was 1234.
```

##### 4. Custom patterns
Custom parsing patterns can be used
```bash
echo "custom; pattern::2345" | ./comet -p "%{LD:field_1}; %{LD:field_2}::%{INT:field_3}" "select * from events where field_3 = 2345"
```

```json
{"data":"custom; pattern::2345","field_3":2345,"field_1":"custom","field_2":"pattern","eventType":"events"}
```

##### 5. Multiple EPL queries
Multiple queries can be run on same data.  
Two events are sent to `comet`. First query outputs one, second output other event.  
```bash
echo $'foo\t514\tstarting'$'\n'$'bar\t514\tstopping' | \
    ./comet \
    -p "%{LD:source}\t%{INT:id}\t%{LD:action}"\
    "select * from events where action = 'starting'" \
    "select * from events where action = 'stopping'"
```
``` json
{"data":"foo\t514\tstarting","action":"starting","source":"foo","id":514,"eventType":"events"}
{"data":"bar\t514\tstopping","action":"stopping","source":"bar","id":514,"eventType":"events"}
```

##### 6. EPL from files (+ more complex queries)  
Esper queries can be read from files.  
Multiple lines can be joined in esper epl. 
```sql
# syntax highlighted query
select
    begin.source as start_source,
    stop.source as stop_source,
    begin.id as id
from
    events (action='stopping') as stop unidirectional,
    events(action='starting')#time(10 sec) as begin
where stop.id = begin.id
```
```bash
# copy-paste exec
echo "select begin.source as start_source, stop.source as stop_source, begin.id as id from events (action='stopping') as stop unidirectional, events(action='starting')#time(10 sec) as begin where stop.id = begin.id" > query.epl && \
echo $'foo\t514\tstarting'$'\n'$'bar\t514\tstopping' | \
    ./comet \
    -p "%{LD:source}\t%{INT:id}\t%{LD:action}"\
    query.epl
```
```json
{"stop_source":"bar","id":514,"start_source":"foo"}
```

## Running Comet daemonized

Comet can be started as a background process (so called daemonized instance).

Simple example:  
```bash
./comet --name mymonitor --daemonize
```

## Secrets

Secrets for accessing different external resources can be defined in multiple places. This should cover most of the needs for different use-cases. Order of loading from top to bottom:

```php
  runtime-secrets       # secrets.yaml next to the Comet jar file  
         |
    home-secrets        # secrets.yaml in dir ~/.comet (home dir)
         |
    exec-secrets        # secrets.yaml in the current dir (where Comet was executed)
         |
configuration-secrets   # secrets.yaml next to the configuration file (if used)
         |
  command-line-arg      # file pointed by command-line argument --secrets
```

All found secret files are loaded and merged. If same secret definition exists in multiple files, later one overrides previous. This means that secrets file pointed by command-line argument has the highest priority. Secrets file are defined in yaml and format file is following:  
```yaml
secret-name:
  key: value
  key2: value2
```

## Monitor configuration file

In addition to command line parameters, monitors inside `comet` can be configured via yaml configuration files. These files define how four of the main components of `comet` behave and what they do. One such simple working example can be defined like this:  

```yaml
input: logfile.log # input definitions (read contents of file 'logfile.log')
pattern: ^%{INT:seq}\t%{NOTSPACE:data}$ # how to parse the input (fields 'seq' and 'data')
query: select data from events where seq = 4 # esper statements on parsed data
output: outputfile.out # output definitions (output resulting json to file 'outputfile.out')
```

This is equal to using command line parameters:  
```bash
./comet --file logfile.log -p "^%{TIMESTAMP_ISO8601:timestamp}\t%{INT:seq}\t%{NOTSPACE:data}$" "select data from events where seq = 4" --out-file outputfile.out
```
However configuration files allow far more options and better control over input/ouput redirection.

### General options
These options are not part of the main four components.

```yaml
# set's the name of the monitor  
name: monitor-name # string

# explicitly set external timing
clock: external # type: string; (external|internal); default is internal

# set initial startup timestamp (rarely useful)
initial-time: 1577829600000 # type: long; milliseconds since epoc

# enable logging and set target (stdout or file)
log: logfile.log # type: string; (-|stdout|filename)

# enable debug logging and set target (stdout or file)
debug: debuglog.log # type: string; (-|stdout|filename)

# set basepath for log files
log-path: /path/to/log/folder # type: string; default is current dir

# undocumented feature
debug-enable: foobar # type: string|list

# specify event types to be used in parsing
# useful if using pattern definition files that have lots of event-types defined but only one-two specific will we used (speeds the parsing part a bit)
# parent events will be automatically added to the list
event-types: # type: string|list of strings
```

### Input definitions

Input definition can be a simple string, a map or a list of either of them. A string and list of strings are just a shorthand notation of simple (non-tailing, non-repeating) file input type. All other input types must be defined as a map (or list of maps for multiple input definitions) and must contain at least input type (and can contain input name). Other params depend on the input type.
#### Stdin input

This is the default input that is created if no inputs are defined. Mostly useful for testing purposes.

```yaml
type: stdin
name: input-nice-name # optional
```

#### Static list input

This input type allows to define list of elements that will be defined as a Esper window (with one field). Mostly useful for testing purposes, but can be use to implement some sort of black/whitelists.    

```yaml
type: list
name: commands    # required, name of the event (this will be esper window name)
field: data       # optional, name of the field where data is set (default: data)
data:             # list of elements to be put into window
  - mimikatz
  - powershell
```

#### File input
File input allows to read data from file (or multiple files). There are three different use-case scenarios:
* once - file is read once and then input finishes
* repeated (`repeat: 60`) - repeat the read process every n seconds
* tailing (`tail: true`) - tail the file (starts reading from end)  

Paths to file (property `file`) can be relative or absolute. For relative paths, globbing can be used but then prefix `file:` must be used. Path to files a relative to the configuration file directory.

```yaml
type: file
name: input-nice-name # optional, set display-name for input (optional)

# relative, relative with glob and absolute file definitions
file: filename.log    # required, set input file (relative to config-file). Can be string or list of strings.  
file: file:*.log
file: /path/to/logfile.log

# one of following two can be configured (tail takes precedence if both configured)
tail: true # turn on fail tailing (optional, default is false)
repeat: 10 # turn on repeated-reading (in seconds, default is disabled)
```

#### CSV file input

CSV input can be used to load test or lookup data into Esper window.

Example csvinput.csv:
```csv
cmd,description,severity
mimikatz,a windows powertool,5
```
conf: 
```yaml
type: csv
name: commands      # required, name of the window (or event type, if no window is created)
file: csvinput.csv  # required, path of the file (relative to the configuration file)
content: |          # this can be used instead of 'file: file.csv' 
  field1,field2,field3
  data1,data2,data3
window: true        # optional (default: true), if false - no window is created
header: true        # optional (default: true), if false - first line is assumed to be data (fields must be configured using 'fields' parameter in the correct order)
fields:             # this can be used to override field types (not all fields must be configured), or define fields with types (if no header line is present)
  severity: int
```


### Pattern definitions

Patterns for parsing input data can be described as raw regex (fieldnames are extracted using regex named groups) or using GROK (GROK expressions will be compiled to regex). These expressions are evaluated for every line of input. Every pattern expression describes format for a certain event type. Event types can be nested (event can have multiple parents). Match with the most specific event type is used. All lines match at least `events` event type. `events` is a built-in event type and has regex expression `(?<data>.*)` (GROK equivalent: `%{DATA:data}`). All event types that don't explicitly define their parent have `events` as parent. By default contents of field named `data` is used for evaluating pattern expressions. Events can be for example nested like this:
```
+ events
 + firewall
  - ...
 + logevents
  + apache_logs
   - apache_error
   - apache_access
  + application_logs
   - ...
```
 If line `X` matches `logevents` pattern, then it's child patterns are evaluated and so forward. If in the end line `X` matches pattern `apache_access`, then it has the event type `apache_acces` with all of it's and all of it's parent event type (nested up to `events`) fields.

Depending on the input type, extra filtering can be applied to choose which pattern expressions are evaluated for given line (for example depending on source file- or stream name).

Pattern expressions can be described directly inside main configuration file or in a separate yaml file. A short configuration example inside main configuration file:

```yaml
pattern:
 - name: logevents
   pattern: ^%{TIMESTAMP_ISO8601:timestamp}\t{DATA:data}$

 - name: apache_logs
   parent: logevents
   pattern: ^apache-prefix\t%{NUMBER:some_number}.*$
```

Actual options for defining patterns are following:
```yaml
pattern:
  - name: event-type-name
    parent: parent-event-type # string, or list of strings
    # at least one of patterns has to match to classify as this event type
    pattern: pattern-expression # string, or list of strings
    # if 'pattern' matches, then optional patterns are evaluated for extracting additional fields
    optionalpattern: pattern-expression # string, or list of strings
    # name of the field that is used for evaluating pattern expressions (default: data)
    field: field-name # string
    order: load-order # int
    cond: # map of field/value pairs that all have to match for to evaluate these expressions
      field: value-as-regex
    softcond: # map of field/value pairs that have to match only if this field exists in event
      field: value-as-regex
    replace: # list of maps (used to modify field values, executed after parsing, uses java replaceAll() method)
      - field: field_name
        regex: regex-expression
        replacement: replacement-string
    fields: # map of explicitly defined fields and their types
      field_name: type  # (type can be string,int,long,double,float)
    srctime-field: field-name # string, field name where to find the srctime to parse
    srctime-format: SimpleDateFormat # string (java SimpleDateFormat). if srctime-field contents match this, time in milliseconds is put into 'src_logts_timestamp' field

```

*MORE DOCUMENTATION NEEDED HERE !!!*

### Query definitions

*mostly undocumented*  

A query definition can be inline, reference to a file containing a query or a glob pattern matching files that contain esper queries. Relative paths to files will be relative to configuration file directory. Glob patterns must (direct references may) be prefixed with `file:`. Glob patterns will work only with relative paths! Globs will be processed using patterns described in Java [PathMatcher](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String)). Comet accepts such query definitions as a string or a list of strings.

```yaml
# examples
query: select * from events
query: my-query.epl
query: /with/absolute/path/my-query.epl
query: file:*.epl
query:
  - file:default-query.epl
  - file:custom-*.epl
```

### Output definitions

Output definition can be a simple string, a map or a list of either of them. A string and list of strings are just a shorthand notations of file,stdout (value stdout) or noop (value noop) output types. All other ouptu types must be defined as a map (or list of maps for multiple output definitions) and must contain at least output type (and can contain output name). Other params depend on the output type.

#### stdout output ####

This is the default output that is created if no output's are defined. Mostly useful for testing purposes.

```yaml
type: stdout
name: output-nice-name # optional

# format output using apache-commons-text StringSubstitutor
# if this is not specified, output is in json format
out-format: value is ${fieldname} # optional
```

#### noop output ####

If you need to silence the output somewhy (noop = no operation = /dev/null)...

```yaml
type: noop
name: output-nice-name # optional
```

#### file output ####

```yaml
type: file
name: output-nice-name # optional

# path selection order:
# > if path is absolute, it is used
# > if path is realative, and general log-path is specified then it is used as basepath
# > if path is relative and no log-path is set, configuration file path is used as basepath
file: path-to-output-file.ext # mandatory

# format output using apache-commons-text StringSubstitutor
# if this is not specified, output is in json format
out-format: value is ${fieldname} # optional
```

## Esper query specifics 

### User defined functions

**network/ip functions** - using library https://github.com/seancfoley/IPAddress  

`ipin(ip:string, net:string)` - check if ip is in network  
`isprivateip(ip:string)` - check if ip is in one of the private ranges, this equals:
```sql
    ipin(ip, '172.16.0.0/12') OR
    ipin(ip, '192.168.0.0/16') OR
    ipin(ip, '10.0.0.0/8') OR
    ipin(ip, '169.254.0.0/16') OR
    ipin(ip, '127.0.0.0/8')
```
`isremoteip(ip:string)` - inverse of `isprivateip(ip:string)`  
`dns_lookup(ip:string)` - do reverse dns lookup  
`dns_lookup(domain:string)` - do dns lookup  