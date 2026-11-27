// plugin-temporal is the Temporal backend plugin of Spec Studio. It keeps
// each spec in one Temporal workflow for both hosts (the relay and the app),
// by the protocol in https://github.com/spec-studio-app/plugin-sdk.
//
//	plugin-temporal                          # stdio
//	plugin-temporal --listen 127.0.0.1:7070  # HTTP (set --token or PLUGIN_TOKEN)
//	plugin-temporal --version
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/settings"
	"github.com/spec-studio-app/plugin-temporal/internal/store"
)

// version is set at release time with -ldflags "-X main.version=...".
var version = "0.0.0-dev"

// maxMessageBytes keeps each update and each continue-as-new input under
// Temporal's payload limit (2 MB by default).
const maxMessageBytes = 1536 * 1024

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	runLimit := flag.Int("run-limit", 0, "tests only: continue a run as new after this many updates (default 2000)")
	idle := flag.Duration("idle", 0, "tests only: end a run with no document after this idle time (default 168h)")
	catalogRunLimit := flag.Int("catalog-run-limit", 0, "tests only: the run limit of the catalogs")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}
	plugin.Main(Plugin(store.Options{RunLimit: *runLimit, Idle: *idle, CatalogRunLimit: *catalogRunLimit}))
}

// Plugin gives the plugin with its info, settings and store.
func Plugin(o store.Options) plugin.Plugin {
	if o.Idle < 0 {
		o.Idle = time.Duration(0)
	}
	return plugin.Plugin{
		Info: protocol.PluginInfo{
			Name:    "plugin-temporal",
			Version: version,
			Repo:    "https://github.com/spec-studio-app/plugin-temporal",
		},
		Settings: settings.Declared(),
		Store:    store.New(o),
		Limits:   protocol.Limits{MaxMessageBytes: maxMessageBytes},
	}
}
