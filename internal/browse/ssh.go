package browse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type remoteJSONResponse struct {
	Path   string `json:"path"`
	Parent string `json:"parent"`
	Items  []struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size"`
	} `json:"items"`
}

func BrowseSSH(host string, port int, user, pass, targetPath string) (*BrowseResult, error) {
	cleanHost := strings.TrimSpace(host)
	if cleanHost == "" {
		return nil, fmt.Errorf("SSH Server Host / IP is required to browse")
	}

	if port <= 0 {
		port = 22
	}

	cleanUser := strings.TrimSpace(user)
	cleanPath := strings.TrimSpace(targetPath)

	// Attempt 1: Native Go SSH client
	res, err := browseSSHNative(cleanHost, port, cleanUser, pass, cleanPath)
	if err == nil && res != nil {
		return res, nil
	}

	// Attempt 2: Fallback to system OpenSSH binary
	resCLI, errCLI := browseSSHCLI(cleanHost, port, cleanUser, pass, cleanPath)
	if errCLI == nil && resCLI != nil {
		return resCLI, nil
	}

	if err != nil {
		return nil, fmt.Errorf("SSH connection failed: %w", err)
	}
	return nil, fmt.Errorf("SSH connection failed: %w", errCLI)
}

func getSSHAuthMethods(pass string) []ssh.AuthMethod {
	var methods []ssh.AuthMethod
	if pass != "" {
		methods = append(methods, ssh.Password(pass))
	}

	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if agentConn, err := net.Dial("unix", sock); err == nil {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers))
		}
	}

	if home, err := os.UserHomeDir(); err == nil {
		for _, keyName := range []string{"id_rsa", "id_ed25519", "id_ecdsa", "id_dsa"} {
			keyPath := filepath.Join(home, ".ssh", keyName)
			if keyBytes, err := os.ReadFile(keyPath); err == nil {
				if signer, err := ssh.ParsePrivateKey(keyBytes); err == nil {
					methods = append(methods, ssh.PublicKeys(signer))
				}
			}
		}
	}

	return methods
}

func browseSSHNative(host string, port int, user, pass, targetPath string) (*BrowseResult, error) {
	authMethods := getSSHAuthMethods(pass)
	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no authentication method available (provide password or configure SSH keys)")
	}

	effectiveUser := user
	if effectiveUser == "" {
		if home, err := os.UserHomeDir(); err == nil {
			effectiveUser = filepath.Base(home)
		} else {
			effectiveUser = "root"
		}
	}

	cfg := &ssh.ClientConfig{
		User:            effectiveUser,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	// Try python3 one-liner first
	pyCmd := fmt.Sprintf(`python3 -c "import os, sys, json; p = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else os.path.expanduser('~'); ap = os.path.abspath(p); res = {'path': ap, 'parent': os.path.dirname(ap) if ap != '/' else '', 'items': []}; [res['items'].append({'name': e, 'is_dir': os.path.isdir(os.path.join(ap, e)), 'size': os.path.getsize(os.path.join(ap, e)) if not os.path.isdir(os.path.join(ap, e)) else 0}) for e in sorted(os.listdir(ap)) if not e.startswith('.')]; print(json.dumps(res))" "%s"`, targetPath)

	outBytes, err := session.Output(pyCmd)
	if err == nil {
		var resp remoteJSONResponse
		if jsonErr := json.Unmarshal(outBytes, &resp); jsonErr == nil && resp.Path != "" {
			return formatJSONBrowseResult(resp), nil
		}
	}

	// Shell fallback
	sessionFallback, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	defer sessionFallback.Close()

	shCmd := fmt.Sprintf(`p="%s"; [ -z "$p" ] && p="$HOME"; cd "$p" 2>/dev/null || cd /; pwd; ls -1ap`, targetPath)
	shOut, err := sessionFallback.Output(shCmd)
	if err != nil {
		return nil, err
	}

	return parseShellLSOutput(string(shOut)), nil
}

func browseSSHCLI(host string, port int, user, pass, targetPath string) (*BrowseResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	userHost := host
	if user != "" {
		userHost = user + "@" + host
	}

	remoteScript := fmt.Sprintf(`p="%s"; [ -z "$p" ] && p="$HOME"; cd "$p" 2>/dev/null || cd /; pwd; ls -1ap`, targetPath)

	args := []string{
		"-p", strconv.Itoa(port),
		"-o", "ConnectTimeout=5",
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "NumberOfPasswordPrompts=1",
		userHost,
		remoteScript,
	}

	cmd := exec.CommandContext(ctx, "ssh", args...)
	if pass != "" {
		exe, err := os.Executable()
		if err == nil {
			cmd.Env = append(os.Environ(),
				"SSH_ASKPASS="+exe,
				"SSH_ASKPASS_REQUIRE=force",
				"SYNCHOP_IS_ASKPASS=1",
				"DISPLAY=:0",
				"SYNCHOP_SSH_PASS="+pass,
			)
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errStr := strings.TrimSpace(stderr.String())
		if errStr != "" {
			return nil, fmt.Errorf("%s", errStr)
		}
		return nil, err
	}

	return parseShellLSOutput(stdout.String()), nil
}

func formatJSONBrowseResult(resp remoteJSONResponse) *BrowseResult {
	var items []FileItem
	for _, it := range resp.Items {
		items = append(items, FileItem{
			Name:  it.Name,
			Path:  path.Join(resp.Path, it.Name),
			IsDir: it.IsDir,
			Size:  it.Size,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].IsDir != items[j].IsDir {
			return items[i].IsDir
		}
		return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
	})

	return &BrowseResult{
		CurrentPath: resp.Path,
		ParentPath:  resp.Parent,
		Items:       items,
	}
}

func parseShellLSOutput(out string) *BrowseResult {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		return &BrowseResult{CurrentPath: "/", ParentPath: "", Items: []FileItem{}}
	}

	currentPath := strings.TrimSpace(lines[0])
	if currentPath == "" {
		currentPath = "/"
	}

	parentPath := ""
	if currentPath != "/" {
		parentPath = path.Dir(strings.TrimSuffix(currentPath, "/"))
	}

	var items []FileItem
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || line == "./" || line == "../" || line == "." || line == ".." {
			continue
		}
		if strings.HasPrefix(line, ".") {
			continue // skip hidden
		}

		isDir := strings.HasSuffix(line, "/")
		name := strings.TrimSuffix(line, "/")

		items = append(items, FileItem{
			Name:  name,
			Path:  path.Join(currentPath, name),
			IsDir: isDir,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].IsDir != items[j].IsDir {
			return items[i].IsDir
		}
		return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
	})

	return &BrowseResult{
		CurrentPath: currentPath,
		ParentPath:  parentPath,
		Items:       items,
	}
}
