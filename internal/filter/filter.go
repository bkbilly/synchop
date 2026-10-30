package filter

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type FilterMode string

const (
	ModeAll         FilterMode = "all"
	ModeIncludeOnly FilterMode = "include_only"
	ModeExclude     FilterMode = "exclude"
)

func ParsePatterns(text string) []string {
	var pats []string
	raw := strings.FieldsFunc(text, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	for _, p := range raw {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			pats = append(pats, trimmed)
		}
	}
	return pats
}

type Filter struct {
	Mode        FilterMode
	Patterns    []string
	MaxFileSize int64 // bytes; if > 0, files exceeding this size are excluded
	RsyncSize   string
}

func parseSizePattern(p string) (int64, string, bool) {
	clean := strings.TrimSpace(p)
	clean = strings.TrimPrefix(clean, "max-size=")
	clean = strings.TrimPrefix(clean, "max-size:")
	clean = strings.TrimPrefix(clean, "--max-size=")
	if strings.HasPrefix(clean, ">") {
		clean = strings.TrimSpace(clean[1:])
	} else if !strings.HasPrefix(p, "max-size") && !strings.HasPrefix(p, "--max-size") {
		return 0, "", false
	}

	upper := strings.ToUpper(clean)
	var multiplier int64 = 1
	var numPart string

	if strings.HasSuffix(upper, "KB") || strings.HasSuffix(upper, "K") {
		multiplier = 1024
		numPart = strings.TrimSuffix(strings.TrimSuffix(upper, "B"), "K")
	} else if strings.HasSuffix(upper, "MB") || strings.HasSuffix(upper, "M") {
		multiplier = 1024 * 1024
		numPart = strings.TrimSuffix(strings.TrimSuffix(upper, "B"), "M")
	} else if strings.HasSuffix(upper, "GB") || strings.HasSuffix(upper, "G") {
		multiplier = 1024 * 1024 * 1024
		numPart = strings.TrimSuffix(strings.TrimSuffix(upper, "B"), "G")
	} else if strings.HasSuffix(upper, "TB") || strings.HasSuffix(upper, "T") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numPart = strings.TrimSuffix(strings.TrimSuffix(upper, "B"), "T")
	} else if strings.HasSuffix(upper, "B") {
		multiplier = 1
		numPart = strings.TrimSuffix(upper, "B")
	} else {
		numPart = upper
	}

	val, err := strconv.ParseInt(strings.TrimSpace(numPart), 10, 64)
	if err != nil || val <= 0 {
		return 0, "", false
	}
	return val * multiplier, strings.ToLower(clean), true
}

func NewFilter(modeStr, patternsText string) *Filter {
	mode := FilterMode(modeStr)
	if mode != ModeIncludeOnly && mode != ModeExclude {
		mode = ModeAll
	}
	allPats := ParsePatterns(patternsText)
	var globPats []string
	var maxBytes int64
	var rsyncSizeStr string

	for _, p := range allPats {
		if bytesVal, rsyncStr, ok := parseSizePattern(p); ok {
			maxBytes = bytesVal
			rsyncSizeStr = rsyncStr
		} else {
			globPats = append(globPats, p)
		}
	}

	return &Filter{
		Mode:        mode,
		Patterns:    globPats,
		MaxFileSize: maxBytes,
		RsyncSize:   rsyncSizeStr,
	}
}

// ShouldKeep returns true if the file or directory should be synced
func (f *Filter) ShouldKeep(relPath string, isDir bool) bool {
	if f.Mode == ModeAll || len(f.Patterns) == 0 {
		return true
	}

	cleanPath := filepath.ToSlash(filepath.Clean(relPath))
	cleanPath = strings.TrimPrefix(cleanPath, "./")
	cleanPath = strings.TrimPrefix(cleanPath, "/")
	baseName := filepath.Base(cleanPath)

	if isDir {
		if f.Mode == ModeIncludeOnly {
			// In include-only mode, allow directory traversal to find matching files
			return true
		}
		// In exclude mode, check if directory itself matches an exclude pattern
		for _, pat := range f.Patterns {
			cleanPat := strings.TrimSuffix(pat, "/")
			if matched, _ := filepath.Match(cleanPat, baseName); matched {
				return false
			}
			if matched, _ := filepath.Match(cleanPat, cleanPath); matched {
				return false
			}
		}
		return true
	}

	// For files:
	if f.Mode == ModeIncludeOnly {
		for _, pat := range f.Patterns {
			if matched, _ := filepath.Match(pat, baseName); matched {
				return true
			}
			if matched, _ := filepath.Match(pat, cleanPath); matched {
				return true
			}
		}
		return false
	}

	if f.Mode == ModeExclude {
		for _, pat := range f.Patterns {
			cleanPat := strings.TrimSuffix(pat, "/")
			if matched, _ := filepath.Match(cleanPat, baseName); matched {
				return false
			}
			if matched, _ := filepath.Match(cleanPat, cleanPath); matched {
				return false
			}
			// Check parent directories
			dirPath := filepath.Dir(cleanPath)
			if dirPath != "." && dirPath != "" {
				segments := strings.Split(dirPath, "/")
				for _, seg := range segments {
					if m, _ := filepath.Match(cleanPat, seg); m {
						return false
					}
				}
			}
		}
		return true
	}

	return true
}

// ToRsyncArgs converts filter configuration into rsync command arguments
func ToRsyncArgs(modeStr, patternsText string) []string {
	f := NewFilter(modeStr, patternsText)
	if f.Mode == ModeAll && f.MaxFileSize == 0 {
		return nil
	}

	var args []string
	if f.MaxFileSize > 0 {
		if f.RsyncSize != "" {
			args = append(args, "--max-size="+f.RsyncSize)
		} else {
			args = append(args, fmt.Sprintf("--max-size=%d", f.MaxFileSize))
		}
	}

	if f.Mode == ModeIncludeOnly {
		args = append(args, "--include=*/")
		for _, pat := range f.Patterns {
			args = append(args, "--include="+pat)
		}
		args = append(args, "--exclude=*")
		args = append(args, "--prune-empty-dirs")
		args = append(args, "--delete-excluded")
	} else if f.Mode == ModeExclude {
		for _, pat := range f.Patterns {
			args = append(args, "--exclude="+pat)
		}
		args = append(args, "--delete-excluded")
	}
	return args
}

// ToRsyncArgsForSource converts source-level filters into rsync arguments anchored to that source folder in staging
func ToRsyncArgsForSource(sourceName, modeStr, patternsText string) []string {
	f := NewFilter(modeStr, patternsText)
	if f.Mode == ModeAll && f.MaxFileSize == 0 {
		return nil
	}

	sourceName = strings.Trim(filepath.ToSlash(sourceName), "/")
	var args []string

	if f.MaxFileSize > 0 {
		if f.RsyncSize != "" {
			args = append(args, "--max-size="+f.RsyncSize)
		} else {
			args = append(args, fmt.Sprintf("--max-size=%d", f.MaxFileSize))
		}
	}

	if f.Mode == ModeExclude {
		for _, pat := range f.Patterns {
			cleanPat := strings.TrimPrefix(pat, "/")
			if sourceName != "" {
				if strings.Contains(cleanPat, "/") {
					args = append(args, fmt.Sprintf("--filter=- /%s/%s", sourceName, cleanPat))
				} else {
					args = append(args, fmt.Sprintf("--filter=- /%s/**%s", sourceName, cleanPat), "--exclude="+cleanPat)
				}
			} else {
				args = append(args, "--exclude="+cleanPat)
			}
		}
		args = append(args, "--delete-excluded")
	} else if f.Mode == ModeIncludeOnly {
		if sourceName != "" {
			args = append(args, fmt.Sprintf("--include=/%s/**/", sourceName))
			for _, pat := range f.Patterns {
				cleanPat := strings.TrimPrefix(pat, "/")
				if strings.Contains(cleanPat, "/") {
					args = append(args, fmt.Sprintf("--include=/%s/%s", sourceName, cleanPat))
				} else {
					args = append(args, fmt.Sprintf("--include=/%s/**%s", sourceName, cleanPat))
				}
			}
			args = append(args, fmt.Sprintf("--exclude=/%s/**", sourceName))
			args = append(args, "--prune-empty-dirs")
			args = append(args, "--delete-excluded")
		} else {
			return ToRsyncArgs(modeStr, patternsText)
		}
	}
	return args
}

// WalkDirWithSymlinks recursively walks root, resolving top-level or directory symlinks
func WalkDirWithSymlinks(root string, walkFn func(relPath string, info os.FileInfo, fullPath string) error) error {
	var walkSub func(currentDir, currentRel string) error
	walkSub = func(currentDir, currentRel string) error {
		entries, err := os.ReadDir(currentDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			subPath := filepath.Join(currentDir, name)
			subRel := filepath.Join(currentRel, name)

			fi, err := entry.Info()
			if err != nil {
				continue
			}

			// If symlink, resolve target info
			if fi.Mode()&os.ModeSymlink != 0 {
				if realFi, err := os.Stat(subPath); err == nil {
					fi = realFi
				}
			}

			if err := walkFn(subRel, fi, subPath); err != nil {
				if err == filepath.SkipDir {
					continue
				}
				return err
			}

			if fi.IsDir() {
				if err := walkSub(subPath, subRel); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walkSub(root, "")
}
