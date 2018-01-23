[![Coverage Status](https://img.shields.io/coveralls/thrawn01/args.svg)](https://coveralls.io/github/thrawn01/args)
[![Build Status](https://img.shields.io/travis/thrawn01/args/master.svg)](https://travis-ci.org/thrawn01/args)
[![Code Climate](https://codeclimate.com/github/thrawn01/args/badges/gpa.svg)](https://codeclimate.com/github/thrawn01/args)

**NOTE: This is alpha software, the api will continue to evolve until the 1.0 release**

## Introduction
A cloud native app configuration and argument parser designed for
 use in micro services with support for CLI clients and live server
 configuration reloading

## Configuration Philosophy
Configuration for modern cloud native applications is divided into two separate
phases. The first phase is initialization, This is the minimum configuration
the service needs to start up. At a minimum this may include configuration for:
Service Interface, Port Number, and the source location of the
second phase configuration.

The second phase includes everything else the service needs to operate.
Including: Monitoring, Logging, Databases, etc.... Everything in the First
Phase should be mostly static items; things that can not change unless the
service is restarted. Everything in the second phase can and should change at
anytime during the operation of the service without the need for the service to
restart.

Args as a project has the following goals

1. Config option access should be thread safe. This allows new requests to a
   service to retrieve the newest version of the config on a per request basis.
2. Backend config watchers. Args should provide an interface to plugin key/value stores
   such as etcd and zookeeper with helper functions designed to make updating
   your local config super simple.
3. Easy to use subcommand support. I have years of experience using and writing
   subcommand like tools. Take my lessions learned and apply them here. [See
   subcommand.org](http://subcommand.org)
4. The relationship between, Environment, Commandline, and Config should NOT
   result in impedance mismatch. If a feature would result in an imedance
   mismatch the feature will be rejected. This results in args being somewhat
   opinionated. However, for the operators sake, cloud native applications
   should be opinionated and intuitive in how they are configured and operated.


***NOTE: Some of these features are still Work In Progress, and the API will
evolve until we have a full 1.0 release***

## Installation
```
go get github.com/thrawn01/args
```

## Development Guide
Args uses can use glide to ensure the proper dependencies are installed, but args
should compile without it.

Fetch the source
```
go get -d github.com/thrawn01/args
cd $GOPATH/src/github.com/thrawn01/args
```
Install glide and fetch the dependencies via glide
```
make get-deps
```
Run make to build the example and run the tests
```
make
```
## Thread safe option access
Options can be retrieved in a thread safe manner by calling ```GetOpts()```.
Using ```GetOpts()``` and ```SetOpts()``` allows the user to control when new
configs are applied to the service.

```go
parser := args.NewParser()
parser.AddConfig("some-key").Alias("-k").Default("default-key").
    Help("A fake api-key")

// Parses the commandline, calls os.Exit(1) if there is an error
opts := parser.ParseOrExit(nil)

// Simple handler that returns some-key
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    // GetOpts is a thread safe way to get the current options
    conf := parser.GetOpts()

    // Marshal the response to json
    payload, err := json.Marshal(map[string]interface{}{
        "some-key": conf.String("some-key"),
    })
    // Write the response to the user
    w.Header().Set("Content-Type", "application/json")
    w.Write(payload)
})
```

## Watch key store backends for config changes
Args supports additional backend configuration via any backend that implements the
 ```Backend``` interface. Currently only etcd is supported and is provided by the
  [args-etcd](http://github.com/thrawn01/args-etcd) repo.

```go
// -- snip ---
client, err := etcdv3.New(etcd.Config{
    Endpoints:   opts.StringSlice("etcd-endpoints"),
    DialTimeout: 5 * time.Second,
})
if err != nil {
    log.Fatal(err)
}

backend := backends.NewV3Backend(client, "/etcd-endpoints-service")

// Read all the available config values from etcd
opts, err = parser.FromBackend(backend)
if err != nil {
    fmt.Printf("Etcd error - %s\n", err.Error())
}

// Watch etcd for any configuration changes (This starts a go routine)
cancelWatch := parser.Watch(backend, func(event args.ChangeEvent, err error) {
    if err != nil {
        fmt.Println(err.Error())
        return
    }
    fmt.Printf("Change Event - %+v\n", event)
    // This takes a ChangeEvent and updates the opts with the latest changes
    parser.Apply(opts.FromChangeEvent(event))
})
// -- snip ---
```

## Command Support
The following code creates a command, and sub-command such that usage works like this
```
$ my-cli show
Show things here

$ my-cli volume create my-new-volume
Volume 'my-new-volume' created

```

```go
func main() {
    parser := args.NewParser(args.Name("subcommand"),
        args.Desc("Example subcommand CLI"))

    // Add a subcommand
    parser.AddCommand("show", show)

    // Add a sub-sub-command
    parser.AddCommand("volume", func(subParser *args.ArgParser, data interface{}) int {
        subParser.AddCommand("create", createVolume)

        // Run the sub-commands
        retCode, err := subParser.ParseAndRun(nil, data)
        if err != nil {
            fmt.Println(err.Error())
            return 1
        }
        return retCode
    })

    // Run the command chosen by the user
    retCode, err := parser.ParseAndRun(nil, nil)
    if err != nil {
        fmt.Fprintln(os.Stderr, err.Error())
        os.Exit(1)
    }
    os.Exit(retCode)
}
func show(subParser *args.ArgParser, data interface{}) int {
    fmt.Printf("Show things here\n")
    return 0
}

func createVolume(subParser *args.ArgParser, data interface{}) int {
    subParser.AddArgument("name").Required().Help("The name of the volume to create")
    opts, err := subParser.Parse(nil)
    if err != nil {
        fmt.Println(err.Error())
        return 1
    }

    // Create our volume
    if err := volume.Create(opts.String("name")); err != nil {
        fmt.Fprintln(os.Stderr, err)
        return 1
    }
    fmt.Printf("Volume '%s' created\n")
    return 0
}
```

## Watch Config with hot reload
Args can reload your config when modifications are made to a watched config file. **This works well
with Kubernetes ConfigMap**
```go
    parser := args.NewParser()
    parser.AddOption("--config").Alias("-c").Help("Read options from a config file")
    opt := parser.ParseSimple(nil)
    configFile := opt.String("config")
    // Initial load of our config file
    opt, err = parser.FromIniFile(configFile)

    // check our config file for changes every second
    cancelWatch, err := args.WatchFile(configFile, time.Second, func(err error) {
        if err != nil {
            fmt.Printf("Error Watching %s - %s", configFile, err.Error())
            return
        }

        fmt.Println("Config file changed, Reloading...")
        opt, err = parser.FromIniFile(configFile)
        if err != nil {
            fmt.Printf("Failed to load config - %s\n", err.Error())
            return
        }
    })
    if err != nil {
        fmt.Printf("Failed to watch '%s' -  %s", configFile, err.Error())
    }
    // Shut down the watcher when done
    defer cancelWatch()
```

## Demo Code Examples
See more code examples in the ```examples/``` directory



## Stuff that works
* Support list of strings '--list my,list,of,things'
* Support Counting the number of times an arg has been seen
* Support for Storing Strings,Ints,Booleans in a struct
* Support Default Arguments
* Support Reading arguments from an ini file
* Support different types of optional prefixes (--, -, ++, +, etc..)
* If AddOption() is called with a name that doesn’t begin with a prefix, apply some default rules to match - or -— prefix
* Support for Config only options
* Support for Groups
* Support for Etcd v3 (See: https://github.com/thrawn01/args-backends)
* Support for Watching Etcd v3 for changes and hot reload (See: https://github.com/thrawn01/args-backends)
* Support Positional Arguments
* Support for adhoc configuration groups using AddConfigGroups()
* Generate Help Message
* Support SubCommands
* Support Nested SubCommands
* Automatically adds a --help message if none defined
* Support for escaping arguments (IE: --help and \\-\\-help are different)
* Automatically generates help for SubCommands
* Tests for args.WatchFile()
* Support list of strings '--list my,list,of,things'
* Support map type '--map={1:"thing", 2:"thing"}'
* Support Greedy Arguments ```[<files>….]```
* Support Parent Parsing
* Support for Kubernetes ConfigMap file watching

## TODO
* Custom Help and Usage
* Support counting arguments in this format -vvvv
* Support float type '--float=3.14'
* Support '-arg=value'
* Write better intro document
* Write godoc
* Ability to include Config() options in help message
* Add support for updating etcd values from the Option{} object. (shouldn't be hard)
