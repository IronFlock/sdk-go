package ironflock

import (
	"fmt"
	"os"
	"strings"

	"github.com/ironflock/sdk-go/internal/env"
)

// cloudStudioURL is the Studio of the IronFlock cloud. A device whose
// STUDIO_URL is this one is a cloud device; with any other Studio it is
// attached to an appliance.
const cloudStudioURL = "https://studio.ironflock.com"

// cloudTunnelDomain is the public tunnel edge of the IronFlock cloud.
const cloudTunnelDomain = "app.ironflock.com"

// maxDNSLabelLength is the longest single DNS label (RFC 1035), which a
// tunnel label must fit in.
const maxDNSLabelLength = 63

// GetRemoteAccessURLForPort returns the public URL of a port declared in
// the app's port-template.yml, once its tunnel is active. protocol is
// "http" (default when empty), "https", "tcp" or "udp". Port values are read
// live from /data/env, so call it again rather than caching the result. It
// returns false when the URL cannot be composed.
//
// The URL is on the tunnel domain of the platform the device belongs to, and
// STUDIO_URL, which the device agent injects, alone decides which that is:
//
//   - STUDIO_URL is https://studio.ironflock.com: a device of the IronFlock
//     cloud. The domain is the cloud's tunnel edge, app.ironflock.com.
//   - Any other STUDIO_URL: a device attached to an appliance. The domain is
//     the appliance's own tunnel domain, which the agent injects as
//     TUNNEL_DOMAIN. Such a device always gets the appliance's tunnel URL,
//     never one on the cloud's edge — also while the appliance forwards its
//     tunnels to the cloud (INSTANCE_KEY and the _CLOUD remote ports play no
//     part).
//
// http and https ports are served under the tunnel label
// <DEVICE_KEY>-<app name, lower case>-<port> (https ports with the
// platform's "secure-" prefix, a development run's ports with a "-dev"
// suffix) on that domain, always as an https:// URL. The label must be a
// single valid DNS label: at most 63 characters, no dot (an app name
// containing a dot cannot be tunneled).
//
// tcp and udp ports use the public port the tunnel assigned, which the
// device agent announces as REMOTE_PORT_FOR_<port> (agent 0.19.8 and later):
// <protocol>://<tunnel domain>:<remote port>. remotePortEnvironment, when not
// empty, names the variable to read instead: the template's own
// remote_port_environment, which agents before 0.19.8 need. A port of 0 has
// not been assigned yet.
//
// Where TUNNEL_DOMAIN is unset or empty (no device agent, or one before
// 0.19.2), the domain is app.ironflock.com. On an edge device of an appliance
// installed without a domain (plain mode) the agent injects
// TUNNEL_DOMAIN=localhost, so the URLs point at localhost: such an appliance
// serves app ports on its host ports, which a tunnel URL cannot express.
//
// The stage is the one of the realm the app joins (see Stage). In a DEV
// container the http(s) URL is the development run's own tunnel, whose label
// ends in -dev (https://42-myapp-8080-dev.app.ironflock.com), so it never
// collides with the PROD install of the same app on this device; agents
// before 1.4.9 tunnel no development run, so it is not served there. A
// tcp/udp URL is the development run's own too: its tunnel comes up after the
// container started, and the agent announces its remote port in /data/env
// only while it is up, so the result is false until then.
//
// The values come from /data/env, which older agents make readable by root
// only: in a container there running as another user they are the values the
// container started with, and the SDK logs a warning once.
func (f *IronFlock) GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		protocol = "http"
	}

	switch protocol {
	case "tcp", "udp":
		name := remotePortEnvironment
		if name == "" {
			name = fmt.Sprintf("REMOTE_PORT_FOR_%d", port)
		}
		remotePort, err := env.Lookup(name)
		env.WarnUnreadable(f.log, err)
		p, ok := assignedPort(remotePort)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%s://%s:%s", protocol, tunnelDomain(), p), true
	case "http", "https":
	default:
		return "", false
	}

	if f.deviceKey == "" || f.appName == "" {
		return "", false
	}
	label := fmt.Sprintf("%s-%s-%d", f.deviceKey, strings.ToLower(f.appName), port)
	if f.stage == StageDevelopment {
		// A development run's tunnel carries -dev after the port, so it
		// never collides with the PROD install's (CreateSubdomain).
		label += "-dev"
	}
	if protocol == "https" {
		// https-protocol tunnels get secure- prefixed subdomains platform-wide.
		label = "secure-" + label
	}
	if len(label) > maxDNSLabelLength || strings.Contains(label, ".") {
		return "", false
	}
	return fmt.Sprintf("https://%s.%s", label, tunnelDomain()), true
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

// tunnelDomain returns the domain this device's tunnels are published under.
//
// STUDIO_URL alone decides between the cloud's and an appliance's: a device
// of the IronFlock cloud (STUDIO_URL is https://studio.ironflock.com) has the
// cloud's tunnel edge, and every other device — one attached to an appliance
// — the appliance's own tunnel domain, which the device agent injects as
// TUNNEL_DOMAIN. A device of an appliance never gets a URL on the cloud's
// edge, whether or not the appliance forwards its tunnels to the cloud.
//
// Without TUNNEL_DOMAIN (no device agent, or one before 0.19.2) the cloud's
// edge is the only domain known.
func tunnelDomain() string {
	studioURL := strings.ToLower(strings.TrimRight(strings.TrimSpace(os.Getenv("STUDIO_URL")), "/"))
	if studioURL == cloudStudioURL {
		return cloudTunnelDomain
	}
	if d := os.Getenv("TUNNEL_DOMAIN"); d != "" {
		return d
	}
	return cloudTunnelDomain
}
