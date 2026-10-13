package ironflock

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ironflock/sdk-go/internal/env"
)

// remoteFlock is an IronFlock with device key 42 and app name MyApp.
func remoteFlock(t *testing.T, env map[string]string) *testFlock {
	t.Helper()
	setIdentityEnv(t)
	t.Setenv("DEVICE_KEY", "42")
	t.Setenv("APP_NAME", "MyApp")
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
		{"instance", map[string]string{"INSTANCE_KEY": "5"}, 1881, "http", "https://i5-42-myapp-1881.app.ironflock.com"},
		{"instance ignores local domain", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local"}, 1881, "http", "https://i5-42-myapp-1881.app.ironflock.com"},
		{"instance cloud edge", map[string]string{"INSTANCE_KEY": "5", "CLOUD_TUNNEL_DOMAIN": "app.ironflock.dev"}, 1881, "http", "https://i5-42-myapp-1881.app.ironflock.dev"},
		{"https", nil, 8443, "https", "https://secure-42-myapp-8443.app.ironflock.com"},
		{"https instance", map[string]string{"INSTANCE_KEY": "5"}, 8443, "https", "https://i5-secure-42-myapp-8443.app.ironflock.com"},
		{"protocol case", nil, 8443, "HTTPS", "https://secure-42-myapp-8443.app.ironflock.com"},
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

func TestRemoteAccessURLUnavailable(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		port     int
		protocol string
		portEnv  string
	}{
		{"label longer than 63", map[string]string{"APP_NAME": strings.Repeat("a", 60)}, 8080, "http", ""},
		{"instance label longer than 63", map[string]string{"APP_NAME": strings.Repeat("a", 50), "INSTANCE_KEY": "12345"}, 8080, "http", ""},
		{"dotted app name", map[string]string{"APP_NAME": "my.app"}, 8080, "http", ""},
		{"no device key", map[string]string{"DEVICE_KEY": ""}, 8080, "http", ""},
		{"no app name", map[string]string{"APP_NAME": ""}, 8080, "http", ""},
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
		{"instance without cloud port", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local", "MY_TCP_PORT": "30012"}, "tcp", "MY_TCP_PORT", "tcp://appliance.local:30012"},
		{"instance cloud port", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local", "MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:31099"},
		{"instance cloud port, cloud edge", map[string]string{"INSTANCE_KEY": "5", "CLOUD_TUNNEL_DOMAIN": "edge.example", "MY_TCP_PORT_CLOUD": "31099"}, "tcp", "MY_TCP_PORT", "tcp://edge.example:31099"},
		{"cloud port ignored without instance", map[string]string{"MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
		{"no device identity needed", map[string]string{"DEVICE_KEY": "", "APP_NAME": "", "MY_TCP_PORT": "30012"}, "tcp", "MY_TCP_PORT", "tcp://app.ironflock.com:30012"},
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

func TestRemoteAccessURLPicksUpTheCloudPortWithoutRestart(t *testing.T) {
	f := remoteFlock(t, map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local", "MY_TCP_PORT": "30012"})
	dir := os.Getenv("IRONFLOCK_ENV_DIR")
	url := func() string {
		got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT")
		return got
	}
	if got := url(); got != "tcp://appliance.local:30012" {
		t.Fatalf("before the cloud port: %q", got)
	}
	cloud := filepath.Join(dir, "MY_TCP_PORT_CLOUD.txt")
	if err := os.WriteFile(cloud, []byte("31099"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := url(); got != "tcp://app.ironflock.com:31099" {
		t.Errorf("after the agent wrote the cloud port: %q", got)
	}
	if err := os.Remove(cloud); err != nil {
		t.Fatal(err)
	}
	if got := url(); got != "tcp://appliance.local:30012" {
		t.Errorf("after the cloud port was removed: %q", got)
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
// REMOTE_PORT_FOR_<port> (plus REMOTE_PORT_FOR_<port>_CLOUD on instances), the
// name the platform docs give: a tcp/udp port declared without
// remote_port_environment has a URL too.
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
		{"instance cloud port", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local",
			"REMOTE_PORT_FOR_1700": "30006", "REMOTE_PORT_FOR_1700_CLOUD": "31099"}, nil, 1700, "udp", "udp://app.ironflock.com:31099"},
		{"instance before the cloud port", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local",
			"REMOTE_PORT_FOR_1700": "30006"}, nil, 1700, "udp", "udp://appliance.local:30006"},
		{"instance cloud port 0", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local",
			"REMOTE_PORT_FOR_1700": "30006", "REMOTE_PORT_FOR_1700_CLOUD": "0"}, nil, 1700, "udp", "udp://appliance.local:30006"},
		// What refreshRemotePortEnvFiles writes for rules without
		// remote_port_environment, on an instance device.
		{"the agent's files", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local"},
			map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "REMOTE_PORT_FOR_5900": "30005", "REMOTE_PORT_FOR_1700": "30006",
				"REMOTE_PORT_FOR_1700_CLOUD": "31099"}, 1700, "udp", "udp://app.ironflock.com:31099"},
		{"the agent's files, no cloud port", map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local"},
			map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "REMOTE_PORT_FOR_5900": "30005"}, 5900, "tcp", "tcp://appliance.local:30005"},
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

// When an instance's cloud port goes away (forwarding turned off, the tunnel
// gone), the agent deletes <name>_CLOUD.txt from its mirror. The variable of
// the same name keeps the port the container started with: it must not bring
// the dead cloud URL back.
func TestRemoteAccessURLDropsACloudPortTheAgentRemoved(t *testing.T) {
	startEnv := map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local",
		"MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"}
	url := func(f *testFlock) string {
		got, _ := f.GetRemoteAccessURLForPort(5000, "tcp", "MY_TCP_PORT")
		return got
	}

	f := remoteFlock(t, startEnv)
	writeEnvFiles(t, map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "MY_TCP_PORT": "30012", "MY_TCP_PORT_CLOUD": "31099"})
	if got := url(f); got != "tcp://app.ironflock.com:31099" {
		t.Fatalf("with the cloud port: %q", got)
	}
	if err := os.Remove(filepath.Join(os.Getenv("IRONFLOCK_ENV_DIR"), "MY_TCP_PORT_CLOUD.txt")); err != nil {
		t.Fatal(err)
	}
	if got := url(f); got != "tcp://appliance.local:30012" {
		t.Errorf("after the agent removed the cloud port: %q, want the instance-local URL", got)
	}
	writeEnvFiles(t, map[string]string{"MY_TCP_PORT_CLOUD": ""})
	if got := url(f); got != "tcp://appliance.local:30012" {
		t.Errorf("with an empty cloud port file: %q", got)
	}

	// Without the agent's mirror the environment is all there is.
	f = remoteFlock(t, startEnv)
	if got := url(f); got != "tcp://app.ironflock.com:31099" {
		t.Errorf("no mirror: %q", got)
	}
	writeEnvFiles(t, map[string]string{"APP_AUTH_ID": "app-20-dev-e1@sn", "APP_AUTH_SECRET": "s"})
	if got := url(f); got != "tcp://app.ironflock.com:31099" {
		t.Errorf("a directory without the agent's marker: %q", got)
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
	f := remoteFlock(t, map[string]string{"INSTANCE_KEY": "5", "TUNNEL_DOMAIN": "appliance.local", "MY_TCP_PORT": "30012"})
	writeEnvFiles(t, map[string]string{"DEVICE_SERIAL_NUMBER": "sn", "MY_TCP_PORT_CLOUD": "31099"})
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
