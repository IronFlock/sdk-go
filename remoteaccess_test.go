package ironflock

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ironflock/sdk-go/internal/env"
)

// remoteFlock is an IronFlock with device key 42 and app name MyApp, a PROD
// install unless env sets ENV.
func remoteFlock(t *testing.T, env map[string]string) *testFlock {
	t.Helper()
	setIdentityEnv(t)
	t.Setenv("DEVICE_KEY", "42")
	t.Setenv("APP_NAME", "MyApp")
	t.Setenv("ENV", "PROD")
	for k, v := range env {
		t.Setenv(k, v)
	}
	return newTestFlock(t)
}

func TestRemoteAccessURLForHTTPPorts(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		port     int
		protocol string
		want     string
	}{
		{"cloud default", nil, 8080, "", "https://42-myapp-8080.app.ironflock.com"},
		{"explicit http", nil, 8080, "http", "https://42-myapp-8080.app.ironflock.com"},
		{"tunnel domain", map[string]string{"TUNNEL_DOMAIN": "app.ironflock.dev"}, 8080, "http", "https://42-myapp-8080.app.ironflock.dev"},
		{"empty tunnel domain", map[string]string{"TUNNEL_DOMAIN": ""}, 8080, "http", "https://42-myapp-8080.app.ironflock.com"},
		{"https", nil, 8443, "https", "https://secure-42-myapp-8443.app.ironflock.com"},
		{"protocol case", nil, 8443, "HTTPS", "https://secure-42-myapp-8443.app.ironflock.com"},
		{"prod in any case", map[string]string{"ENV": "prod"}, 8080, "http", "https://42-myapp-8080.app.ironflock.com"},
		// A development run's label ends in -dev, after the port (the agent's
		// CreateSubdomain): the PROD install keeps its own.
		{"dev", map[string]string{"ENV": "DEV"}, 51821, "http", "https://42-myapp-51821-dev.app.ironflock.com"},
		{"dev https", map[string]string{"ENV": "DEV"}, 8443, "https", "https://secure-42-myapp-8443-dev.app.ironflock.com"},
		{"dev cloud device", map[string]string{"ENV": "DEV", "STUDIO_URL": cloudStudio}, 1881, "http", "https://42-myapp-1881-dev.app.ironflock.com"},
		{"dev appliance device", map[string]string{"ENV": "DEV", "STUDIO_URL": applianceStudio, "TUNNEL_DOMAIN": "appliance.example", "INSTANCE_KEY": "5"},
			8443, "https", "https://secure-42-myapp-8443-dev.appliance.example"},
		{"dev tunnel domain", map[string]string{"ENV": "DEV", "TUNNEL_DOMAIN": "appliance.local"}, 8080, "http", "https://42-myapp-8080-dev.appliance.local"},
		{"no ENV is dev", map[string]string{"ENV": ""}, 8080, "http", "https://42-myapp-8080-dev.app.ironflock.com"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteFlock(t, tc.env)
			got, ok := f.GetRemoteAccessURLForPort(tc.port, tc.protocol, "")
			if !ok || got != tc.want {
				t.Errorf("got %q, %v; want %q", got, ok, tc.want)
			}
		})
	}
}

// The Studio URLs of the cloud and of an appliance, as the agent injects them.
const (
	cloudStudio     = "https://studio.ironflock.com"
	applianceStudio = "https://studio.appliance.example"
)

// STUDIO_URL alone decides the tunnel domain: the cloud's edge for the
// cloud's Studio, the appliance's own (TUNNEL_DOMAIN) for any other. A device
// of an appliance gets the appliance's tunnel URL only — INSTANCE_KEY and the
// _CLOUD remote ports play no part.
func TestRemoteAccessURLFollowsTheStudioURL(t *testing.T) {
	appliance := func(extra map[string]string) map[string]string {
		env := map[string]string{"STUDIO_URL": applianceStudio, "TUNNEL_DOMAIN": "appliance.example"}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}
	cases := []struct {
		name     string
		env      map[string]string
		port     int
		protocol string
		portEnv  string
		want     string
	}{
		{"a cloud device", map[string]string{"STUDIO_URL": cloudStudio, "TUNNEL_DOMAIN": "app.ironflock.com"},
			1881, "http", "", "https://42-myapp-1881.app.ironflock.com"},
		// Told from an appliance by the Studio alone: the cloud's edge, though
		// TUNNEL_DOMAIN names another domain.
		{"the cloud Studio with a trailing slash", map[string]string{"STUDIO_URL": cloudStudio + "/", "TUNNEL_DOMAIN": "appliance.example"},
			1881, "http", "", "https://42-myapp-1881.app.ironflock.com"},
		{"the cloud Studio, padded", map[string]string{"STUDIO_URL": " " + cloudStudio + " ", "TUNNEL_DOMAIN": "appliance.example"},
			1881, "http", "", "https://42-myapp-1881.app.ironflock.com"},
		{"the cloud Studio in another case", map[string]string{"STUDIO_URL": "https://Studio.IronFlock.com", "TUNNEL_DOMAIN": "appliance.example"},
			1881, "http", "", "https://42-myapp-1881.app.ironflock.com"},
		// INSTANCE_KEY makes no appliance of a cloud device.
		{"a cloud device with an instance key", map[string]string{"STUDIO_URL": cloudStudio, "INSTANCE_KEY": "5"},
			1881, "http", "", "https://42-myapp-1881.app.ironflock.com"},
		{"a cloud device, tcp", map[string]string{"STUDIO_URL": cloudStudio, "INSTANCE_KEY": "5", "REMOTE_PORT_FOR_5900": "30005"},
			5900, "tcp", "", "tcp://app.ironflock.com:30005"},

		{"an appliance device", appliance(nil), 1881, "http", "", "https://42-myapp-1881.appliance.example"},
		{"an appliance device, https", appliance(nil), 8443, "https", "", "https://secure-42-myapp-8443.appliance.example"},
		// An appliance that forwards its tunnels to the cloud: the agent
		// injects INSTANCE_KEY, and the route
		// https://i5-42-myapp-1881.app.ironflock.com exists. The device's URL
		// is the appliance's all the same.
		{"an appliance device never gets the cloud-forwarded route", appliance(map[string]string{"INSTANCE_KEY": "5"}),
			1881, "http", "", "https://42-myapp-1881.appliance.example"},
		// The same for tcp/udp: the _CLOUD companion is the port on the
		// cloud's edge.
		{"an appliance device never gets the cloud port",
			appliance(map[string]string{"INSTANCE_KEY": "5", "MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"}),
			5000, "tcp", "MY_TCP_PORT", "tcp://appliance.example:30012"},

		{"another cloud's Studio is an appliance", map[string]string{"STUDIO_URL": "https://studio.ironflock.dev", "TUNNEL_DOMAIN": "tunnel.example"},
			8080, "http", "", "https://42-myapp-8080.tunnel.example"},
		{"a Studio that only starts like the cloud's", map[string]string{"STUDIO_URL": cloudStudio + ".appliance.example", "TUNNEL_DOMAIN": "tunnel.example"},
			8080, "http", "", "https://42-myapp-8080.tunnel.example"},
		{"the cloud's host over http", map[string]string{"STUDIO_URL": "http://studio.ironflock.com", "TUNNEL_DOMAIN": "tunnel.example"},
			8080, "http", "", "https://42-myapp-8080.tunnel.example"},
		{"a local Studio", map[string]string{"STUDIO_URL": "http://localhost:8086", "TUNNEL_DOMAIN": "tunnel.example"},
			8080, "http", "", "https://42-myapp-8080.tunnel.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteFlock(t, tc.env)
			got, ok := f.GetRemoteAccessURLForPort(tc.port, tc.protocol, tc.portEnv)
			if !ok || got != tc.want {
				t.Errorf("got %q, %v; want %q", got, ok, tc.want)
			}
		})
	}

	// The agent's mirror announces the cloud port too: not used either.
	f := remoteFlock(t, appliance(map[string]string{"INSTANCE_KEY": "5", "REMOTE_PORT_FOR_1700": "30006"}))
	writeEnvFiles(t, map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "REMOTE_PORT_FOR_1700": "30006", "REMOTE_PORT_FOR_1700_CLOUD": "31099"})
	if got, _ := f.GetRemoteAccessURLForPort(1700, "udp", ""); got != "udp://appliance.example:30006" {
		t.Errorf("with the cloud port in the agent's mirror: %q", got)
	}
}

func TestRemoteAccessURLUnavailable(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		port     int
		protocol string
		portEnv  string
	}{
		{"label longer than 63", map[string]string{"APP_NAME": strings.Repeat("a", 60)}, 8080, "http", ""},
		{"dotted app name", map[string]string{"APP_NAME": "my.app"}, 8080, "http", ""},
		{"no device key", map[string]string{"DEVICE_KEY": ""}, 8080, "http", ""},
		{"no app name", map[string]string{"APP_NAME": ""}, 8080, "http", ""},
		// 63 characters for PROD, 67 with the -dev suffix.
		{"dev label longer than 63", map[string]string{"ENV": "DEV", "APP_NAME": strings.Repeat("a", 63-len("42--8080"))}, 8080, "http", ""},
		{"unknown protocol", nil, 5000, "sctp", ""},
		{"tcp port not announced", nil, 5000, "tcp", ""},
		{"tcp port not injected", nil, 5000, "tcp", "NOT_SET_ANYWHERE"},
		// The agent announces a rule's port before it has reserved one.
		{"tcp port 0", map[string]string{"REMOTE_PORT_FOR_5000": "0"}, 5000, "tcp", ""},
		{"custom port 0", map[string]string{"MY_TCP_PORT": "0"}, 5000, "tcp", "MY_TCP_PORT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteFlock(t, tc.env)
			if got, ok := f.GetRemoteAccessURLForPort(tc.port, tc.protocol, tc.portEnv); ok || got != "" {
				t.Errorf("got %q, %v; want no URL", got, ok)
			}
		})
	}

	// Exactly 63 characters is still a valid label.
	f := remoteFlock(t, map[string]string{"APP_NAME": strings.Repeat("a", 63-len("42--8080"))})
	if got, ok := f.GetRemoteAccessURLForPort(8080, "http", ""); !ok {
		t.Errorf("63-character label refused: %q", got)
	}
}

func TestRemoteAccessURLForTCPAndUDPPorts(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		protocol string
		portEnv  string
		want     string
	}{
		{"tcp cloud device", map[string]string{"MY_TCP_PORT": "30012"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
		{"udp", map[string]string{"WG_PORT": "30013"}, "udp", "WG_PORT", "udp://app.ironflock.com:30013"},
		{"tunnel domain", map[string]string{"MY_TCP_PORT": "30012", "TUNNEL_DOMAIN": "appliance.local"}, "tcp", "MY_TCP_PORT", "tcp://appliance.local:30012"},
		{"a cloud port is ignored", map[string]string{"MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
		{"no device identity needed", map[string]string{"DEVICE_KEY": "", "APP_NAME": "", "MY_TCP_PORT": "30012"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
		// The URL names the remote port, not a label: no stage in it.
		{"dev", map[string]string{"ENV": "DEV", "MY_TCP_PORT": "30012"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteFlock(t, tc.env)
			got, ok := f.GetRemoteAccessURLForPort(5000, tc.protocol, tc.portEnv)
			if !ok || got != tc.want {
				t.Errorf("got %q, %v; want %q", got, ok, tc.want)
			}
		})
	}
}

func TestRemoteAccessURLReadsLiveEnvFiles(t *testing.T) {
	f := remoteFlock(t, map[string]string{"MY_TCP_PORT": "30012"})
	dir := os.Getenv("IRONFLOCK_ENV_DIR")

	// The agent's live file wins over the start-time environment variable.
	if err := os.WriteFile(filepath.Join(dir, "MY_TCP_PORT.txt"), []byte("30099\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT"); got != "tcp://app.ironflock.com:30099" {
		t.Errorf("got %q", got)
	}
	// An empty file counts as absent.
	if err := os.WriteFile(filepath.Join(dir, "MY_TCP_PORT.txt"), []byte("  \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT"); got != "tcp://app.ironflock.com:30012" {
		t.Errorf("got %q", got)
	}
}

// A development run's tunnel comes up after its container started: the agent
// then writes the port's file into the running container, and removes it when
// the tunnel goes.
func TestRemoteAccessURLPicksUpAPortAnnouncedAfterTheStart(t *testing.T) {
	f := remoteFlock(t, map[string]string{"TUNNEL_DOMAIN": "appliance.local"})
	dir := os.Getenv("IRONFLOCK_ENV_DIR")
	url := func() string {
		got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "")
		return got
	}
	if got := url(); got != "" {
		t.Fatalf("before the port: %q", got)
	}
	port := filepath.Join(dir, "REMOTE_PORT_FOR_5000.txt")
	if err := os.WriteFile(port, []byte("30012"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := url(); got != "tcp://appliance.local:30012" {
		t.Errorf("after the agent wrote the port: %q", got)
	}
	if err := os.Remove(port); err != nil {
		t.Fatal(err)
	}
	if got := url(); got != "" {
		t.Errorf("after the port was removed: %q", got)
	}
}

func TestRemoteAccessURLWithoutEnvDirFallsBackToTheEnvironment(t *testing.T) {
	f := remoteFlock(t, map[string]string{"IRONFLOCK_ENV_DIR": "/definitely/not/here", "MY_TCP_PORT": "30012"})
	if got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT"); got != "tcp://app.ironflock.com:30012" {
		t.Errorf("got %q", got)
	}
}

// writeEnvFiles writes the mirror files (name to content, without .txt) into
// the test's IRONFLOCK_ENV_DIR, as the device agent writes them.
func writeEnvFiles(t *testing.T, files map[string]string) {
	t.Helper()
	dir := os.Getenv("IRONFLOCK_ENV_DIR")
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".txt"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// Since agent 0.19.8 every port rule's tunnel port is announced as
// REMOTE_PORT_FOR_<port>, the name the platform docs give: a tcp/udp port
// declared without remote_port_environment has a URL too.
func TestRemoteAccessURLReadsTheCanonicalPortName(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		files    map[string]string
		port     int
		protocol string
		want     string
	}{
		{"tcp", map[string]string{"REMOTE_PORT_FOR_5900": "30005"}, nil, 5900, "tcp", "tcp://app.ironflock.com:30005"},
		{"udp, tunnel domain", map[string]string{"REMOTE_PORT_FOR_1700": "30006", "TUNNEL_DOMAIN": "appliance.local"}, nil,
			1700, "udp", "udp://appliance.local:30006"},
		// What refreshRemotePortEnvFiles writes for rules without
		// remote_port_environment.
		{"the agent's files", map[string]string{"TUNNEL_DOMAIN": "appliance.local"},
			map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "REMOTE_PORT_FOR_5900": "30005", "REMOTE_PORT_FOR_1700": "30006"},
			1700, "udp", "udp://appliance.local:30006"},
		{"the agent's file over the start-time variable", map[string]string{"TUNNEL_DOMAIN": "appliance.local", "REMOTE_PORT_FOR_5900": "30005"},
			map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "REMOTE_PORT_FOR_5900": "30007"}, 5900, "tcp", "tcp://appliance.local:30007"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteFlock(t, tc.env)
			writeEnvFiles(t, tc.files)
			got, ok := f.GetRemoteAccessURLForPort(tc.port, tc.protocol, "")
			if !ok || got != tc.want {
				t.Errorf("got %q, %v; want %q", got, ok, tc.want)
			}
		})
	}

	// A template's own name is still read, and only it: agents before 0.19.8
	// announce nothing else.
	f := remoteFlock(t, map[string]string{"VNC_REMOTE_PORT": "30007", "REMOTE_PORT_FOR_5900": "30005"})
	if got, _ := f.GetRemoteAccessURLForPort(5900, "tcp", "VNC_REMOTE_PORT"); got != "tcp://app.ironflock.com:30007" {
		t.Errorf("custom name: %q", got)
	}
}

// A container that runs as another user than root cannot read the agent's
// mirror: the URL comes from the start-time environment, and the SDK says so
// once.
func TestRemoteAccessURLWarnsOnceWhenTheMirrorIsUnreadable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("file permissions do not apply to this process")
	}
	env.ResetWarning()
	t.Cleanup(env.ResetWarning)
	f := remoteFlock(t, map[string]string{"TUNNEL_DOMAIN": "appliance.local", "MY_TCP_PORT": "30012"})
	writeEnvFiles(t, map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "MY_TCP_PORT": "30099"})
	dir := os.Getenv("IRONFLOCK_ENV_DIR")
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	for range 3 {
		if got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT"); got != "tcp://appliance.local:30012" {
			t.Fatalf("got %q, want the URL of the start-time environment", got)
		}
	}
	if n := strings.Count(f.logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", n, f.logs)
	}
	mustContain(t, f.logs.String(), "permission denied", "readable by root only")
}
