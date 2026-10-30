package wamp

import (
	"cmp"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/ironflock/sdk-go/internal/env"
)

// Stage is the deployment stage of an app's data realm.
type Stage string

const (
	// StageDevelopment selects the app's development realm.
	StageDevelopment Stage = "DEV"
	// StageProduction selects the app's production realm.
	StageProduction Stage = "PROD"
)

// Lower returns the stage as used in realm names and cross-app catalogs:
// "dev" or "prod".
func (s Stage) Lower() string { return strings.ToLower(string(s)) }

// StageFromEnv maps an ENV value to a Stage: "PROD" in any case is
// production; anything else — "DEV", empty, or an unknown value — is
// development.
//
// This one mapping names the realm the app joins
// (realm-<swarm>-<app>-<dev|prod>), fills the stage segment of its
// device-function URIs and is the default stage of its cross-app connections.
func StageFromEnv(value string) Stage {
	if strings.EqualFold(strings.TrimSpace(value), "PROD") {
		return StageProduction
	}
	return StageDevelopment
}

// RealmName returns the data realm of an app: realm-<swarm>-<app>-<dev|prod>.
func RealmName(swarmKey, appKey int, stage Stage) string {
	return fmt.Sprintf("realm-%d-%d-%s", swarmKey, appKey, stage.Lower())
}

// WebSocket router URIs of the known IronFlock deployments.
const (
	StudioWSURI    = "wss://cbw.ironflock.com/ws-ua-usr"
	StudioDevWSURI = "wss://cbw.ironflock.dev/ws-ua-usr"
	StudioWSURIOld = "wss://cbw.record-evolution.com/ws-ua-usr"
	DatapodsWSURI  = "wss://cbw.datapods.io/ws-ua-usr"
	LocalhostWSURI = "ws://localhost:8080/ws-ua-usr"
	// DockerHostWSURI is used when RESWARM_URL is host.docker.internal: the
	// app then runs in a bridge-network container (mac/windows dev stack),
	// where localhost is the container itself.
	DockerHostWSURI = "ws://host.docker.internal:8080/ws-ua-usr"
)

// socketURIMap maps a studio URL (RESWARM_URL) to its router's WebSocket URI.
// It is the union of the Python and JavaScript SDK maps.
var socketURIMap = map[string]string{
	"https://studio.datapods.io":          DatapodsWSURI,
	"https://studio.ironflock.dev":        StudioDevWSURI,
	"https://studio.ironflock.com":        StudioWSURI,
	"https://studio.record-evolution.com": StudioWSURIOld,
	"http://localhost:8085":               LocalhostWSURI,
	"http://localhost:8086":               LocalhostWSURI,
	"http://host.docker.internal:8086":    DockerHostWSURI,
}

// WebSocketURI resolves the router URL an app connects to.
//
//  1. DEVICE_ENDPOINT_URL (set by the device agent for every app container),
//     with its path replaced by /ws-ua-usr.
//  2. reswarmURL (falls back to the RESWARM_URL environment variable when
//     empty), looked up in the map of known studio deployments.
//  3. The public IronFlock cloud router when neither is set.
//
// An unknown studio URL is an error rather than a silent fallback to the
// public cloud: it is typically an appliance or private deployment, whose app
// would otherwise connect to the wrong router. Set DEVICE_ENDPOINT_URL or pass
// an explicit URL instead.
func WebSocketURI(reswarmURL string) (string, error) {
	if endpoint := os.Getenv("DEVICE_ENDPOINT_URL"); endpoint != "" {
		if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" && u.Host != "" {
			u.Path = "/ws-ua-usr"
			u.RawPath = ""
			return strings.TrimSuffix(u.String(), "/"), nil
		}
	}

	if reswarmURL == "" {
		reswarmURL = os.Getenv("RESWARM_URL")
	}
	if reswarmURL == "" {
		return StudioWSURI, nil
	}
	if mapped, ok := socketURIMap[reswarmURL]; ok {
		return mapped, nil
	}
	known := make([]string, 0, len(socketURIMap))
	for k := range socketURIMap {
		known = append(known, k)
	}
	sort.Strings(known)
	return "", fmt.Errorf(
		"cannot resolve WebSocket URI for RESWARM_URL=%q. Set DEVICE_ENDPOINT_URL to the "+
			"deployment's WAMP router URL (e.g. ws://<host>:18080/ws-re-dev) or pass an explicit "+
			"URL (ironflock.WithURL). Known cloud URLs: %s",
		reswarmURL, strings.Join(known, ", "))
}

// SerialNumber returns serialNumber, or the DEVICE_SERIAL_NUMBER environment
// variable when it is empty. It fails when neither is set.
func SerialNumber(serialNumber string) (string, error) {
	if serialNumber != "" {
		return serialNumber, nil
	}
	if s := os.Getenv("DEVICE_SERIAL_NUMBER"); s != "" {
		return s, nil
	}
	return "", fmt.Errorf("ENV Variable 'DEVICE_SERIAL_NUMBER' is not set! " +
		"Set it or pass the serial number explicitly (ironflock.WithSerialNumber)")
}

// AppCredentials returns the (authid, secret) pair an app presents when it
// joins a data realm.
//
// It prefers the PER-APP credential the device agent injects (APP_AUTH_ID /
// APP_AUTH_SECRET, read file-first from /data/env so a rotated credential is
// picked up on the next connection attempt). That is what lets the platform
// authorize this app rather than only its device, and what makes cross-app
// access grants binding.
//
// It falls back to the legacy device-wide pair (serial, serial) when no
// per-app credential was injected. Legacy credentials still authenticate the
// app on its own realm; cross-app access requires the per-app pair.
//
// A /data/env file that exists but cannot be read — older agents make the
// directory readable by root only, so an app container running as another
// user cannot read it — falls back to the environment variable, the value
// the container started with. The first such failure in the process is
// logged as a warning on slog.Default(), unless the ironflock package has
// reported one on its own logger already (the warning is logged once per
// process).
func AppCredentials(serialNumber string) (authID, secret string) {
	authID, idErr := env.Lookup("APP_AUTH_ID")
	secret, secretErr := env.Lookup("APP_AUTH_SECRET")
	env.WarnUnreadable(slog.Default(), cmp.Or(idErr, secretErr))
	if authID != "" && secret != "" {
		return authID, secret
	}
	return serialNumber, serialNumber
}
