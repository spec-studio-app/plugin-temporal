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
	// plugin.Main parses the flags (it adds --listen and --token), so the
	// store reads these when it opens.
	flag.Bool("version", false, "print the version and exit")
	runLimit := flag.Int("run-limit", 0, "tests only: continue a run as new after this many updates (default 2000)")
	idle := flag.Duration("idle", 0, "tests only: end a run with no document after this idle time (default 168h)")
	catalogRunLimit := flag.Int("catalog-run-limit", 0, "tests only: the run limit of the catalogs")
	for _, a := range os.Args[1:] {
		if a == "--version" || a == "-version" {
			fmt.Println(version)
			os.Exit(0)
		}
	}
	plugin.Main(Plugin(func() store.Options {
		return store.Options{RunLimit: *runLimit, Idle: max(*idle, 0), CatalogRunLimit: *catalogRunLimit}
	}))
}

// Plugin gives the plugin with its info, settings and store.
func Plugin(opts func() store.Options) plugin.Plugin {
	return plugin.Plugin{
		Info: protocol.PluginInfo{
			Name:    "plugin-temporal",
			Version: version,
			Repo:    "https://github.com/spec-studio-app/plugin-temporal",
		},
		Settings: settings.Declared(),
		Store:    store.New(opts),
		Limits:   protocol.Limits{MaxMessageBytes: maxMessageBytes},
	}
}
