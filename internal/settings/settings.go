// Package settings declares and checks the settings of plugin-temporal. The
// names are the relay's Temporal and bucket settings in lowercase, and the
// checks and messages are the relay's (src/config.ts in spec-studio-app/relay).
// A message names a setting and never its value.
package settings

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// The names of the settings.
const (
	Address         = "address"
	Namespace       = "namespace"
	AuthName        = "auth"
	APIKey          = "api_key"
	TLSCert         = "tls_cert"
	TLSKey          = "tls_key"
	PayloadKeys     = "payload_keys"
	DeployID        = "deploy_id"
	TaskQueue       = "task_queue"
	BucketEndpoint  = "bucket_endpoint"
	BucketName      = "bucket_name"
	BucketAccessKey = "bucket_access_key"
	BucketSecretKey = "bucket_secret_key"
	BucketRegion    = "bucket_region"
	BlobDir         = "blob_dir"
)

// The defaults, as in the relay.
const (
	DefaultNamespace    = "default"
	DefaultDeployID     = "default"
	DefaultBucketRegion = "auto"
)

// Declared gives the settings that the plugin declares in plugin.describe.
func Declared() []protocol.Setting {
	return []protocol.Setting{
		{Name: Address, Description: "The Temporal address, host:port. For Temporal Cloud: <namespace>.<account>.tmprl.cloud:7233, or the regional API key endpoint.", Required: true},
		{Name: Namespace, Description: "The Temporal namespace. Default: default."},
		{Name: AuthName, Description: "How the plugin connects: plain, tls, mtls or api-key. Default: api-key when api_key is set, mtls when tls_cert is set, else plain."},
		{Name: APIKey, Description: "The Temporal Cloud API key (auth api-key).", Secret: true},
		{Name: TLSCert, Description: "The mTLS client certificate: PEM text, or the path of a PEM file (auth mtls)."},
		{Name: TLSKey, Description: "The mTLS client key: PEM text, or the path of a PEM file (auth mtls).", Secret: true},
		{Name: PayloadKeys, Description: "The payload keys: data:<id>:<base64 key>,config:<id>:<base64 key>. The same value as the relay's PAYLOAD_KEYS. The first key of each scope encrypts; every key decrypts.", Required: true, Secret: true},
		{Name: DeployID, Description: "The relay's deploy ID. It is part of each workflow ID. Default: default."},
		{Name: TaskQueue, Description: "The task queue. Default: relay-<deploy_id>. The relay config uses <task_queue>-config."},
		{Name: BucketEndpoint, Description: "The URL of an S3-compatible bucket service for blobs and history exports."},
		{Name: BucketName, Description: "The bucket name."},
		{Name: BucketAccessKey, Description: "The bucket access key ID.", Secret: true},
		{Name: BucketSecretKey, Description: "The bucket secret access key.", Secret: true},
		{Name: BucketRegion, Description: "The bucket region. Default: auto."},
		{Name: BlobDir, Description: "A local folder for blobs and history exports, when no bucket is set (the app on one Mac)."},
	}
}

// Auth is how the plugin connects to Temporal.
type Auth string

// The auth kinds.
const (
	AuthPlain  Auth = "plain"
	AuthTLS    Auth = "tls"
	AuthMTLS   Auth = "mtls"
	AuthAPIKey Auth = "api-key"
)

// PayloadKey is one AES-256 key with its ID.
type PayloadKey struct {
	ID  string
	Key []byte
}

// Keys are the data keys (spec, desk and other workflows) and the config keys
// (the relay-config workflow). The first key of each list encrypts.
type Keys struct {
	Data   []PayloadKey
	Config []PayloadKey
}

// Bucket is an S3-compatible bucket.
type Bucket struct {
	Endpoint  string
	Name      string
	AccessKey string
	SecretKey string
	Region    string
}

// Settings are the checked settings.
type Settings struct {
	Address   string
	Namespace string
	Auth      Auth
	APIKey    string
	TLSCert   []byte
	TLSKey    []byte
	Keys      Keys
	DeployID  string
	TaskQueue string
	// ConfigTaskQueue is TaskQueue with "-config".
	ConfigTaskQueue string
	// Bucket is nil when no bucket setting is set.
	Bucket  *Bucket
	BlobDir string
}

func bad(format string, args ...any) error {
	return protocol.NewError(protocol.BadSettings, format, args...)
}

var (
	keyIDRE    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
	deployIDRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)
)

// Parse checks the settings of a handshake. The SDK has checked already that
// each required setting has a value and that no unknown setting is present.
func Parse(m map[string]string) (*Settings, error) {
	get := func(name string) (string, bool) {
		v, ok := m[name]
		v = strings.TrimSpace(v)
		return v, ok && v != ""
	}
	s := &Settings{}
	var ok bool
	if s.Address, ok = get(Address); !ok {
		return nil, bad("The setting %s is missing. Set the Temporal address, host:port.", Address)
	}
	if s.Namespace, ok = get(Namespace); !ok {
		s.Namespace = DefaultNamespace
	}
	if s.DeployID, ok = get(DeployID); !ok {
		s.DeployID = DefaultDeployID
	}
	if !deployIDRE.MatchString(s.DeployID) {
		return nil, bad("The setting %s must be 1 to 40 lowercase letters, digits or -, and start with a letter or digit.", DeployID)
	}
	if s.TaskQueue, ok = get(TaskQueue); !ok {
		s.TaskQueue = "relay-" + s.DeployID
	}
	s.ConfigTaskQueue = s.TaskQueue + "-config"

	keysText, ok := get(PayloadKeys)
	if !ok {
		return nil, bad("The setting %s is missing. Use the relay's PAYLOAD_KEYS value, or make one with `relay keys`.", PayloadKeys)
	}
	keys, err := ParsePayloadKeys(keysText)
	if err != nil {
		return nil, err
	}
	s.Keys = keys

	if err := parseAuth(s, get); err != nil {
		return nil, err
	}
	if s.Bucket, err = parseBucket(get); err != nil {
		return nil, err
	}
	s.BlobDir, _ = get(BlobDir)
	if s.Bucket == nil && s.BlobDir == "" {
		return nil, bad("The plugin has no place for blobs. Set the four bucket settings (%s, %s, %s, %s), or %s.",
			BucketEndpoint, BucketName, BucketAccessKey, BucketSecretKey, BlobDir)
	}
	return s, nil
}

func parseAuth(s *Settings, get func(string) (string, bool)) error {
	apiKey, hasAPIKey := get(APIKey)
	cert, err := pem(get, TLSCert)
	if err != nil {
		return err
	}
	key, err := pem(get, TLSKey)
	if err != nil {
		return err
	}
	if (cert == nil) != (key == nil) {
		if cert != nil {
			return bad("The setting %s is missing. Set it with %s.", TLSKey, TLSCert)
		}
		return bad("The setting %s is missing. Set it with %s.", TLSCert, TLSKey)
	}
	if hasAPIKey && cert != nil {
		return bad("Set %s or %s, not both.", APIKey, TLSCert)
	}
	auth := AuthPlain
	if hasAPIKey {
		auth = AuthAPIKey
	} else if cert != nil {
		auth = AuthMTLS
	}
	if given, ok := get(AuthName); ok {
		switch Auth(given) {
		case AuthPlain, AuthTLS, AuthMTLS, AuthAPIKey:
		default:
			return bad("The setting %s must be plain, tls, mtls or api-key.", AuthName)
		}
		auth = Auth(given)
		if auth == AuthAPIKey && !hasAPIKey {
			return bad("The setting %s is missing. The setting %s is api-key.", APIKey, AuthName)
		}
		if auth == AuthMTLS && cert == nil {
			return bad("The setting %s is missing. The setting %s is mtls.", TLSCert, AuthName)
		}
		if (auth == AuthPlain || auth == AuthTLS) && (hasAPIKey || cert != nil) {
			return bad("The setting %s is %s, but %s or %s is set.", AuthName, auth, APIKey, TLSCert)
		}
	}
	s.Auth, s.APIKey, s.TLSCert, s.TLSKey = auth, apiKey, cert, key
	return nil
}

// pem reads PEM text, or the file at the path in the setting.
func pem(get func(string) (string, bool), name string) ([]byte, error) {
	text, ok := get(name)
	if !ok {
		return nil, nil
	}
	if strings.HasPrefix(text, "-----BEGIN") {
		return []byte(text), nil
	}
	b, err := os.ReadFile(text)
	if err != nil {
		return nil, bad("The setting %s is not PEM text or the path of a readable file.", name)
	}
	return b, nil
}

func parseBucket(get func(string) (string, bool)) (*Bucket, error) {
	names := []string{BucketEndpoint, BucketName, BucketAccessKey, BucketSecretKey}
	var set, missing []string
	for _, n := range names {
		if _, ok := get(n); ok {
			set = append(set, n)
		} else {
			missing = append(missing, n)
		}
	}
	if len(set) == 0 {
		return nil, nil
	}
	if len(missing) > 0 {
		return nil, bad("Missing bucket settings: %s. Set all four bucket settings, or none.", strings.Join(missing, ", "))
	}
	b := &Bucket{}
	b.Endpoint, _ = get(BucketEndpoint)
	if u, err := url.Parse(b.Endpoint); err != nil || u.Scheme == "" || u.Host == "" {
		return nil, bad("The setting %s is not a valid URL.", BucketEndpoint)
	}
	b.Name, _ = get(BucketName)
	b.AccessKey, _ = get(BucketAccessKey)
	b.SecretKey, _ = get(BucketSecretKey)
	var ok bool
	if b.Region, ok = get(BucketRegion); !ok {
		b.Region = DefaultBucketRegion
	}
	return b, nil
}

// ParsePayloadKeys parses the relay's PAYLOAD_KEYS format: a comma-separated
// list of <scope>:<key id>:<base64 key>, with the scope data or config.
func ParsePayloadKeys(text string) (Keys, error) {
	var keys Keys
	ids := map[string]bool{}
	for _, part := range strings.Split(text, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f := strings.Split(part, ":")
		if len(f) != 3 || (f[0] != "data" && f[0] != "config") || f[1] == "" || f[2] == "" {
			return Keys{}, bad("The setting %s is not valid. Each entry must be data:<id>:<base64 key> or config:<id>:<base64 key>.", PayloadKeys)
		}
		if !keyIDRE.MatchString(f[1]) {
			return Keys{}, bad("The setting %s has a key ID that is not valid. Use 1 to 32 letters, digits, - or _.", PayloadKeys)
		}
		if ids[f[1]] {
			return Keys{}, bad("The setting %s has the same key ID two times.", PayloadKeys)
		}
		ids[f[1]] = true
		key, err := base64.StdEncoding.DecodeString(f[2])
		if err != nil {
			key, err = base64.RawStdEncoding.DecodeString(f[2])
		}
		if err != nil || len(key) != 32 {
			return Keys{}, bad("The setting %s has a key that is not 32 bytes of base64.", PayloadKeys)
		}
		k := PayloadKey{ID: f[1], Key: key}
		if f[0] == "data" {
			keys.Data = append(keys.Data, k)
		} else {
			keys.Config = append(keys.Config, k)
		}
	}
	if len(keys.Data) == 0 {
		return Keys{}, bad("The setting %s has no data key.", PayloadKeys)
	}
	if len(keys.Config) == 0 {
		return Keys{}, bad("The setting %s has no config key.", PayloadKeys)
	}
	return keys, nil
}

// String never shows a secret.
func (s *Settings) String() string {
	return fmt.Sprintf("address=%s namespace=%s auth=%s deploy_id=%s task_queue=%s bucket=%t blob_dir=%t",
		s.Address, s.Namespace, s.Auth, s.DeployID, s.TaskQueue, s.Bucket != nil, s.BlobDir != "")
}
