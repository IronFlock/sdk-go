package ironflock

import (
	"fmt"
	"os"
	"strings"

	"github.com/ironflock/sdk-go/internal/env"
)

// defaultTunnelDomain is the public tunnel edge of the IronFlock cloud.
const defaultTunnelDomain = "app.ironflock.com"

// maxDNSLabelLength is the longest single DNS label (RFC 1035), which a
// tunnel label must fit in.
const maxDNSLabelLength = 63

// GetRemoteAccessURLForPort returns the public URL of a port declared in
// the app's port-template.yml, once its tunnel is active. protocol is
// "http" (default when empty), "https", "tcp" or "udp". Port values are read
// live from /data/env, so call it again rather than caching the result. It
// returns false when the URL cannot be composed.
//
// http and https ports are served under the tunnel label
// <DEVICE_KEY>-<app name, lower case>-<port> (https ports with the
// platform's "secure-" prefix) on the tunnel domain the device agent injects
// (TUNNEL_DOMAIN), always as an https:// URL. On a device that belongs to an
// instance (appliance, INSTANCE_KEY set) the cloud-forwarded route
// https://i<INSTANCE_KEY>-<label>.<CLOUD_TUNNEL_DOMAIN> is returned instead.
// The label must be a single valid DNS label: at most 63 characters, no dot
// (an app name containing a dot cannot be tunneled).
//
// tcp and udp ports use the public port the tunnel assigned, which the
// device agent announces as REMOTE_PORT_FOR_<port> (agent 0.19.8 and later):
// <protocol>://<TUNNEL_DOMAIN>:<remote port>. remotePortEnvironment, when not
// empty, names the variable to read instead: the template's own
// remote_port_environment, which agents before 0.19.8 need. On an instance
// device the internet-facing port arrives as the variable's _CLOUD companion
// (REMOTE_PORT_FOR_<port>_CLOUD) and is preferred
// (<protocol>://<CLOUD_TUNNEL_DOMAIN>:<port>); until it appears, and again
// once the agent has removed it from /data/env (cloud forwarding turned off),
// the instance-local URL is returned. A port of 0 has not been assigned yet.
//
// Unset or empty tunnel domains default to app.ironflock.com. The platform
// does not set CLOUD_TUNNEL_DOMAIN today: the instance route and the cloud
// port are always composed on app.ironflock.com unless the app sets
// CLOUD_TUNNEL_DOMAIN itself (in its environment settings), say for an
// appliance whose cloud tunnel host is another one. On an edge device of an
// appliance installed without a domain (plain mode) the agent injects
// TUNNEL_DOMAIN=localhost, so the URLs composed on it — every non-instance
// URL, and an instance device's tcp/udp URL until its _CLOUD port arrives —
// point at localhost: such an appliance serves app ports on its host ports,
// which a tunnel URL cannot express.
//
// The agent tunnels the ports of PROD installs only. In a DEV container the
// http(s) URL is the one of the PROD install of the same app on this device
// (the label has no stage), which never reaches this container; reach a DEV
// app on the device's LAN address instead. tcp/udp return false there, as
// the agent announces no remote port to a DEV container.
//
// The values come from /data/env, which the agent makes readable by root
// only: in a container running as another user they are the values the
// container started with, and the SDK logs a warning once.
func (f *IronFlock) GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		protocol = "http"
	}
	instanceKey := os.Getenv("INSTANCE_KEY")

	switch protocol {
	case "tcp", "udp":
		name := remotePortEnvironment
		if name == "" {
			name = fmt.Sprintf("REMOTE_PORT_FOR_%d", port)
		}
		if instanceKey != "" {
			// The agent deletes the _CLOUD file when the cloud port goes
			// away; the variable of the same name is stale by then.
			cloudPort, err := env.LookupLive(name + "_CLOUD")
			env.WarnUnreadable(f.log, err)
			if p, ok := assignedPort(cloudPort); ok {
				return fmt.Sprintf("%s://%s:%s", protocol, tunnelDomain("CLOUD_TUNNEL_DOMAIN"), p), true
			}
		}
		remotePort, err := env.Lookup(name)
		env.WarnUnreadable(f.log, err)
		p, ok := assignedPort(remotePort)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%s://%s:%s", protocol, tunnelDomain("TUNNEL_DOMAIN"), p), true
	case "http", "https":
	default:
		return "", false
	}

	if f.deviceKey == "" || f.appName == "" {
		return "", false
	}
	label := fmt.Sprintf("%s-%s-%d", f.deviceKey, strings.ToLower(f.appName), port)
	if protocol == "https" {
		// https-protocol tunnels get secure- prefixed subdomains platform-wide.
		label = "secure-" + label
	}
	domain := tunnelDomain("TUNNEL_DOMAIN")
	if instanceKey != "" {
		// Instance route: forwarded through the cloud tunnel edge.
		label = "i" + instanceKey + "-" + label
		domain = tunnelDomain("CLOUD_TUNNEL_DOMAIN")
	}
	if len(label) > maxDNSLabelLength || strings.Contains(label, ".") {
		return "", false
	}
	return fmt.Sprintf("https://%s.%s", label, domain), true
}

// assignedPort returns the remote port an announced value names: none when
// it is empty or 0, which the agent announces for a rule whose tunnel has no
// port reserved yet.
func assignedPort(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return "", false
	}
	return value, true
}

// tunnelDomain returns the tunnel domain the environment variable name holds,
// or the public cloud edge when it is unset or empty.
func tunnelDomain(name string) string {
	if d := os.Getenv(name); d != "" {
		return d
	}
	return defaultTunnelDomain
}
