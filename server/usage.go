// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"flag"
	"fmt"
	"os"
)

var (
	OsExit = os.Exit
)

func configureFlagSet(fs *flag.FlagSet, opts *Options,
	showVersion, showHelp, showTLSHelp *bool,
	signal, configFile *string,
	dbgAndTrace, trcAndVerboseTrc, dbgAndTrcAndVerboseTrc *bool) {
	buf := bytes.NewBuffer([]byte{})
	fs.Usage = func() {
		fmt.Fprintln(os.Stdout, buf.String())
		OsExit(0)
	}
	format := func(format string, v ...any) {
		fmt.Fprintf(buf, format, v...)
	}

	serverFlags := map[string]bool{}
	r := func(name string) string {
		serverFlags[name] = true
		return name
	}
	exists := func(name string) bool {
		_, ok := serverFlags[name]
		return ok
	}
	format("Usage: %s [options]\n", fs.Name())

	// Server Options:
	//     -a, --addr, --net <host>         Bind to host address (default: 0.0.0.0)
	//     -p, --port <port>                Use port for clients (default: 4222)
	//     -n, --name
	//         --server_name <server_name>  Server name (default: auto)
	//     -P, --pid <file>                 File to store PID
	//     -m, --http_port <port>           Use port for http monitoring
	//     -ms,--https_port <port>          Use port for https monitoring
	//     -c, --config <file>              Configuration file
	//     -t                               Test configuration and exit
	//     -sl,--signal <signal>[=<pid>]    Send signal to nats-server process (ldm, stop, quit, term, reopen, reload)
	//                                      pid> can be either a PID (e.g. 1) or the path to a PID file (e.g. /var/run/nats-server.pid)
	//         --client_advertise <string>  Client URL to advertise to other servers
	//         --ports_file_dir <dir>       Creates a ports file in the specified directory (<executable_name>_<pid>.ports).
	format("\nServer Options:\n")
	usage := "Bind to host address (default: 0.0.0.0)"
	fs.StringVar(&opts.Host, r("a"), "", usage)
	fs.StringVar(&opts.Host, r("addr"), "", usage)
	fs.StringVar(&opts.Host, r("net"), "", usage)
	format("\t-a, --addr, --net\t\t\t%s\n", usage)

	usage = "Use port for clients (default: 4222)"
	fs.IntVar(&opts.Port, r("p"), 0, usage)
	fs.IntVar(&opts.Port, r("port"), 0, usage)
	format("\t-p, --port <port>\t\t\t%s\n", usage)

	usage = "Server name (default: auto)"
	fs.StringVar(&opts.ServerName, r("n"), "", "Server name.")
	fs.StringVar(&opts.ServerName, r("name"), "", "Server name.")
	fs.StringVar(&opts.ServerName, r("server_name"), "", "Server name.")
	format("\t-n, --name,--server_name <server_name>\t%s\n", usage)

	usage = "File to store PID"

	fs.StringVar(&opts.PidFile, r("P"), "", usage)
	fs.StringVar(&opts.PidFile, r("pid"), "", usage)
	format("\t-P, --pid <file>\t\t\t%s\n", usage)

	usage = "Use port for http monitoring"
	fs.IntVar(&opts.HTTPPort, r("m"), 0, usage)
	fs.IntVar(&opts.HTTPPort, r("http_port"), 0, usage)
	format("\t-m, --http_port <port>\t\t\t%s\n", usage)

	usage = "Use port for https monitoring"
	fs.IntVar(&opts.HTTPSPort, r("ms"), 0, usage)
	fs.IntVar(&opts.HTTPSPort, r("https_port"), 0, usage)
	format("\t-ms,--https_port <port>\t\t\t%s\n", usage)

	usage = "Configuration file"
	fs.StringVar(configFile, r("c"), "", usage)
	fs.StringVar(configFile, r("config"), "", usage)
	format("\t-c, --config <file>\t\t\t%s\n", usage)

	usage = "Test configuration and exit"
	fs.BoolVar(&opts.CheckConfig, r("t"), false, usage)
	format("\t-t\t\t\t\t\t%s\n", usage)

	usage = "Send signal to nats-server process (ldm, stop, quit, term, reopen, reload)"
	fs.StringVar(signal, r("sl"), "", usage)
	fs.StringVar(signal, r("signal"), "", usage)
	format("\t-sl,--signal <signal>[=<pid>]\t\t%s\n", usage)
	format("\t\t\t\t\t\t <pid> can be either a PID (e.g. 1) or the path to a PID\n")
	format("\t\t\t\t\t\t file (e.g. /var/run/nats-server.pid)\n")

	usage = "Client URL to advertise to other servers"
	fs.StringVar(&opts.ClientAdvertise, r("client_advertise"), "", usage)
	format("\t--client_advertise <string>\t\t%s\n", usage)

	usage = "Creates a ports file in the specified directory (<executable_name>_<pid>.ports)."
	fs.StringVar(&opts.PortsFileDir, r("ports_file_dir"), "", usage)
	format("\t--ports_file_dir <dir>\t\t\t%s\n", usage)

	// Logging Options:
	//     -l, --log <file>                 File to redirect log output
	//     -T, --logtime                    Timestamp log entries (default: true)
	//     -s, --syslog                     Log to syslog or windows event log
	//     -r, --remote_syslog <addr>       Syslog server addr (udp://localhost:514)
	//     -D, --debug                      Enable debugging output
	//     -V, --trace                      Trace the raw protocol
	//     -VV                              Verbose trace (traces system account as well)
	//     -DV                              Debug and trace
	//     -DVV                             Debug and verbose trace (traces system account as well)
	//         --log_size_limit <limit>     Logfile size limit (default: auto)
	//         --max_traced_msg_len <len>   Maximum printable length for traced messages (default: unlimited)
	format("\nLogging Options:\n")

	usage = "File to redirect log output"
	fs.StringVar(&opts.LogFile, r("l"), "", usage)
	fs.StringVar(&opts.LogFile, r("log"), "", usage)
	format("\t-l, --log <file>\t\t%s\n", usage)

	usage = "Timestamp log entries (default: true)"
	fs.BoolVar(&opts.Logtime, r("T"), true, usage)
	fs.BoolVar(&opts.Logtime, r("logtime"), true, usage)
	format("\t-T, --logtime\t\t\t%s\n", usage)

	usage = "Log to syslog or windows event log"
	fs.BoolVar(&opts.Syslog, r("s"), false, usage)
	fs.BoolVar(&opts.Syslog, r("syslog"), false, usage)
	format("\t-s, --syslog\t\t\t%s\n", usage)

	usage = "Syslog server addr (udp://localhost:514)"
	fs.StringVar(&opts.RemoteSyslog, r("r"), "", usage)
	fs.StringVar(&opts.RemoteSyslog, r("remote_syslog"), "", usage)
	format("\t-r, --remote_syslog <addr>\t%s\n", usage)

	usage = "Enable debugging output"
	fs.BoolVar(&opts.Debug, r("D"), false, usage)
	fs.BoolVar(&opts.Debug, r("debug"), false, usage)
	format("\t-D, --debug\t\t\t%s\n", usage)

	usage = "Trace the raw protocol"
	fs.BoolVar(&opts.Trace, r("V"), false, usage)
	fs.BoolVar(&opts.Trace, r("trace"), false, usage)
	format("\t-V, --trace\t\t\t%s\n", usage)

	usage = "Enable verbose trace logging (traces system account as well)."
	fs.BoolVar(trcAndVerboseTrc, r("VV"), false, usage)
	format("\t-VV\t\t\t\t%s\n", usage)

	usage = "Enable debug and trace logging"
	fs.BoolVar(dbgAndTrace, r("DV"), false, usage)
	format("\t-DV\t\t\t\t%s\n", usage)

	usage = "Enable debug and verbose trace logging (traces system account as well)"
	fs.BoolVar(dbgAndTrcAndVerboseTrc, r("DVV"), false, usage)
	format("\t-DVV\t\t\t\t%s\n", usage)

	usage = "Logfile size limit being auto-rotated (default: auto)"
	fs.Int64Var(&opts.LogSizeLimit, r("log_size_limit"), 0, usage)
	format("\t--log_size_limit <limit>\t%s\n", usage)

	usage = "Maximum printable length for traced messages (default: unlimited)"
	fs.IntVar(&opts.MaxTracedMsgLen, r("max_traced_msg_len"), 0, usage)
	format("\t--max_traced_msg_len <len>\t%s\n", usage)

	// JetStream Options:
	//     -js, --jetstream                 Enable JetStream functionality
	//     -sd, --store_dir <dir>           Set the storage directory
	format("\nJetStream Options:\n")

	usage = "Enable JetStream"
	fs.BoolVar(&opts.JetStream, r("js"), false, "Enable JetStream.")
	fs.BoolVar(&opts.JetStream, r("jetstream"), false, "Enable JetStream.")
	format("\t-js, --jetstream\t%s\n", usage)

	usage = "Set the storage directory"
	fs.StringVar(&opts.StoreDir, r("sd"), "", usage)
	fs.StringVar(&opts.StoreDir, r("store_dir"), "", usage)
	format("\t-sd, --store_dir <dir>\t%s\n", usage)

	// Authorization Options:
	//         --user <user>                User required for connections
	//         --pass <password>            Password required for connections
	//         --auth <token>               Authorization token required for connections
	format("\nAuthorization Options:\n")
	usage = "Username required for connections"
	fs.StringVar(&opts.Username, r("user"), "", usage)
	format("\t--user <user>\t\t%s\n", usage)

	usage = "Password required for connections"
	fs.StringVar(&opts.Password, r("pass"), "", usage)
	format("\t--pass <password>\t%s\n", usage)

	usage = "Authorization token required for connections"
	fs.StringVar(&opts.Authorization, r("auth"), "", usage)
	format("\t--auth <token>\t\t%s\n", usage)

	// TLS Options:
	//         --tls                        Enable TLS, do not verify clients (default: false)
	//         --tlscert <file>             Server certificate file
	//         --tlskey <file>              Private key for server certificate
	//         --tlsverify                  Enable TLS, verify client certificates
	//         --tlscacert <file>           Client certificate CA for verification
	format("\nTLS Options:\n")
	usage = "Enable TLS, do not verify clients (default: false)"
	fs.BoolVar(&opts.TLS, r("tls"), false, usage)
	format("\t--tls\t\t\t%s\n", usage)

	usage = "Server certificate file"
	fs.StringVar(&opts.TLSCert, r("tlscert"), "", usage)
	format("\t--tlscert <file>\t%s\n", usage)

	usage = "Private key for server certificate"
	fs.StringVar(&opts.TLSKey, r("tlskey"), "", usage)
	format("\t--tlskey <file>\t\t%s\n", usage)

	usage = "Enable TLS, verify client certificates"
	fs.BoolVar(&opts.TLSVerify, r("tlsverify"), false, usage)
	format("\t--tlsverify\t\t%s\n", usage)

	usage = "Client certificate CA for verification"
	fs.StringVar(&opts.TLSCaCert, r("tlscacert"), "", usage)
	format("\t--tlscacert <file>\t%s\n", usage)

	// Cluster Options:
	//         --routes <rurl-1, rurl-2>    Routes to solicit and connect
	//         --cluster <cluster-url>      Cluster URL for solicited routes
	//         --cluster_name <string>      Cluster Name, if not set one will be dynamically generated
	//         --no_advertise <bool>        Do not advertise known cluster information to clients
	//         --cluster_advertise <string> Cluster URL to advertise to other servers
	//         --connect_retries <number>   For implicit routes, number of connect retries
	//         --cluster_listen <url>       Cluster url from which members can solicit routes
	format("\nCluster Options:\n")

	usage = "Routes to solicit and connect"
	fs.StringVar(&opts.RoutesStr, r("routes"), "", usage)
	format("\t--routes <url1, url2, url3...>\t\t%s\n", usage)
	usage = "Cluster listen URL for solicited routes"
	fs.StringVar(&opts.Cluster.ListenStr, r("cluster"), "", usage)
	fs.StringVar(&opts.Cluster.ListenStr, r("cluster_listen"), "", usage)
	format("\t--cluster, --cluster_listen <listen>\t%s\n", usage)

	usage = "Cluster Name, if not set one will be dynamically generated"
	fs.StringVar(&opts.Cluster.Name, r("cluster_name"), "", usage)
	format("\t--cluster_name <string>\t\t\t%s\n", usage)

	usage = "Do not advertise known cluster information to clients"
	fs.BoolVar(&opts.Cluster.NoAdvertise, r("no_advertise"), false, usage)
	format("\t--no_advertise <bool>\t\t\t%s\n", usage)

	usage = "Cluster URL to advertise to other servers"
	fs.StringVar(&opts.Cluster.Advertise, r("cluster_advertise"), "", usage)
	format("\t--cluster_advertise <string>\t\t%s\n", usage)

	usage = "For implicit routes, number of connect retries"
	fs.IntVar(&opts.Cluster.ConnectRetries, r("connect_retries"), 0, usage)
	format("\t--connect_retries <number>\t\t%s\n", usage)

	// Profiling Options:
	//         --profile <port>             Profiling HTTP port
	format("\nProfiling Options:\n")

	usage = "Profiling HTTP port"
	fs.IntVar(&opts.ProfPort, r("profile"), 0, usage)
	format("\t--profile <port>\t%s\n", usage)

	format("\nCommon Options:\n")
	usage = "Show this message"
	fs.BoolVar(showHelp, r("h"), false, usage)
	fs.BoolVar(showHelp, r("help"), false, usage)
	format("\t-h, --help\t\t%s\n", usage)

	usage = "Show version"
	fs.BoolVar(showVersion, r("version"), false, "Print version information.")
	fs.BoolVar(showVersion, r("v"), false, "Print version information.")
	format("\t-v, --version\t\t%s\n", usage)

	usage = "TLS help"
	fs.BoolVar(showTLSHelp, r("help_tls"), false, usage)
	format("\t--help_tls\t\t%s\n", usage)

	extendedOptions := map[string]string{}
	fs.VisitAll(func(f *flag.Flag) {
		name := f.Name
		usage := f.Usage
		if exists(name) {
			return
		}
		prefix := "-"
		if len(name) > 1 {
			prefix = "--"
		}
		if f.DefValue != "true" && f.DefValue != "false" {
			name = fmt.Sprintf("%s <%s>", name, name)
		}
		extendedOptions[prefix+name] = usage
	})
	if len(extendedOptions) > 0 {
		format("\nExtended Options:\n")
	}
	for name, usage := range extendedOptions {
		format("\t%s\t\t%s\n", name, usage)
	}
}
